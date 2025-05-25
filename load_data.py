import asyncio
import asyncpg
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import Union, Optional, Any, List, Dict
from features.logging import setup_logger
from preprocessing import main_preprocessing
import numpy as np
import json
import joblib

UTC = timezone.utc  # Создаем алиас для совместимости

logger = setup_logger()

DB_DSN = os.getenv('DB_DSN', 'postgresql://student:5SxdeChZ@emcable.teledesk.ru:15432/mscada_db')
CONTROL_TAG = 602
DATA_TAGS = [34, 565, 566, 567, 568, 569, 570, 571, 600, 601, 602, 603, 604]

class PostgresClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5)
        logger.info("Подключение к базе данных PostgreSQL установлено")

    # Получение последнего значения тега снятия со стопа линии
    async def fetch_tag_value(self, tag_id: int) -> int:
        sql = f'''SELECT 
                    archive_itemid, 
                    source_time, 
                    value
                FROM data_raw
                WHERE
                    archive_itemid = {tag_id} AND layer = 0
                ORDER BY source_time DESC LIMIT 1'''
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql)
            if row:
                return (int(row['value']), row['source_time'])
            else:
                logger.warning(f"Данные для тега {tag_id} не найдены")
                return (0, 0)

    
    # Получение значений тегов за период снятия со стопа потекущий момент времени
    async def fetch_multiple_tags(self, tag_ids: List[int], date_begin: str) -> Dict[int, Dict[str, Any]]:
        placeholders = ','.join(f'${i+1}' for i in range(len(tag_ids)))
        date_now = int((datetime.now().timestamp() + 11644473600) * 10000000)
        sql = f'''SELECT 
                    archive_itemid, 
                    source_time, 
                    value
                FROM data_raw
                WHERE
                    archive_itemid IN ({placeholders}) 
                    AND layer IN (0, 1)
                    AND source_time between {date_begin} and {date_now} 
                    ORDER BY source_time '''

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *tag_ids)
            logger.debug(f"Fetched {len(rows)} tag values")
            #return {'archive_itemid': r['archive_itemid'], 'value': r['value'], 'timestamp': r['source_time']} for r in rows}
            data_dict = {
                'archive_itemid': [],
                'value': [],
                'timestamp': []
            }
            for r in rows:
                data_dict['archive_itemid'].append(r['archive_itemid'])
                data_dict['value'].append(r['value'])
                data_dict['timestamp'].append(r['source_time'])

            
            return data_dict


class DataCollector:
    def __init__(self, db: PostgresClient):
        self.db = db
        self.collecting = False
        self.queue: asyncio.Queue = asyncio.Queue()
        self._collector_task: Optional[asyncio.Task] = None
        self.current_session = None
        self.sessions = []

    async def start(self):
        await self.db.connect()
        logger.info("Сборщик данных запущен")
        asyncio.create_task(self._monitor_control())

    async def _monitor_control(self):
        start_ts = 0
        while True:
            val, timestamp = await self.db.fetch_tag_value(CONTROL_TAG)
            logger.info(
                f"ЗНАЧЕНИЕ ТЕГА 602: {val}, timestamp: {timestamp} ({datetime.fromtimestamp((timestamp / 10000000) - 11644473600)})")
            start_ts = timestamp
            if val == 1 and not self.collecting:
                self.collecting = True
                logger.info(f"НАЧАЛО СБОРА ДАННЫХ: timestamp: {timestamp}, start_ts: {start_ts}")
                # Начало новой сессии
                self.current_session = {
                    "start_time": datetime.now(UTC).isoformat(),
                    "end_time": None,
                    "real_emergencies": 0,
                    "predicted_emergencies": 0
                }
                self._collector_task = asyncio.create_task(self._collect_loop(start_ts))
            elif val == 0 and self.collecting:
                self.collecting = False
                logger.info(f"ЗАВЕРШЕНИЕ СБОРА ДАННЫХ: timestamp: {timestamp}, start_ts: {start_ts}")
                # Завершение сессии
                if self.current_session:
                    self.current_session["end_time"] = datetime.now(UTC).isoformat()
                    self.sessions.append(self.current_session)
                    self._save_sessions()
                    self.current_session = None
                if self._collector_task:
                    self._collector_task.cancel()
            await asyncio.sleep(1)

    def _save_sessions(self):
        try:
            filename = "Session_logs.json"
            existing = []
            if os.path.exists(filename):
                with open(filename, "r") as f:
                    existing = json.load(f)

            all_sessions = existing + self.sessions
            with open(filename, "w") as f:
                json.dump(all_sessions, f, indent=2, default=str)

            self.sessions = []
            logger.info(f"Сохранена сессия в {filename}")
        except Exception as e:
            logger.error(f"Ошибка сохранения сессий: {str(e)}")

    def add_real_emergency(self):
        if self.current_session:
            self.current_session["real_emergencies"] += 1

    def add_predicted_emergency(self):
        if self.current_session:
            self.current_session["predicted_emergencies"] += 1

    async def _collect_loop(self, timestamp: str):
        try:
            while self.collecting:
                data = await self.db.fetch_multiple_tags(DATA_TAGS, timestamp)
                timestamped = {
                    'collected_at': datetime.now(UTC).isoformat(),
                    'data': data
                }
                await self.queue.put(timestamped)
                # Log data collection
                logger.info(f"Собрано данных: {len(timestamped['data']['value'])}")

                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Завершение работы потока collector
            logger.info("Завершение работы потока collector")
            pass


class PredictionLogger:
    def __init__(self):
        self.predictions = []
        self.history = []
        self.confirmed_predictions = []
        self.false_predictions = []
        self.logger = logging.getLogger('PredictionLogger')
        self._setup_logger()

    def _setup_logger(self):
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s')

        # Вывод в консоль
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def add_prediction(self, timestamp, lead_time, confidence, emergency_detected):
        prediction_record = {
            'timestamp': timestamp.isoformat(),
            'lead_time_sec': lead_time,
            'confidence': round(confidence, 4),
            'is_emergency_predicted': emergency_detected,
            'is_emergency_actual': None,
            'predicted_time': (timestamp + pd.Timedelta(seconds=lead_time)).strftime('%H:%M:%S'),
            'is_confirmed': None  # Добавляем поле
        }

        # Добавляем в оба списка (history и predictions)
        self.history.append(prediction_record)
        self.predictions.append(prediction_record)  # Если действительно нужно дублирование

        # Логируем информацию
        self.logger.info(
            f"ПРЕДСКАЗАНИЕ: {'🚨 Авария' if emergency_detected else 'Нет аварии'} "
            f"через {lead_time} сек "
            f"(в {prediction_record['predicted_time']}, "
            f"уверенность {confidence:.1%})"
        )

    def check_predictions(self, event_time):
        for pred in self.predictions:
            # Инициализируем поле если его нет
            if 'is_confirmed' not in pred:
                pred['is_confirmed'] = None

            # Преобразуем строку времени обратно в datetime
            try:
                pred_timestamp = datetime.fromisoformat(pred['timestamp'])
                predicted_time = pred_timestamp + timedelta(seconds=pred['lead_time_sec'])
            except Exception as e:
                self.logger.error(f"Ошибка обработки времени: {str(e)}")
                continue

            # Вычисляем разницу с событием
            time_diff = (event_time - predicted_time).total_seconds()

            # Проверка условий
            if abs(time_diff) <= 2:
                pred['is_confirmed'] = True
                self.confirmed_predictions.append(pred)
                self.logger.warning(
                    f"✅ ПОДТВЕРЖДЕНО: Предсказание за {pred['lead_time_sec']} сек "
                    f"(объявлено {pred_timestamp.strftime('%H:%M:%S')})"
                )
            elif event_time > predicted_time:
                pred['is_confirmed'] = False
                self.false_predictions.append(pred)
                self.logger.error(
                    f"❌ ЛОЖНОЕ: Предсказание {pred_timestamp.strftime('%H:%M:%S')} "
                    f"ожидало событие к {predicted_time.strftime('%H:%M:%S')}"
                )

    def log_best_prediction(self):
        if self.confirmed_predictions:
            best = max(self.confirmed_predictions, key=lambda x: x['lead_time'])
            self.logger.warning(
                f"🏆 ЛУЧШЕЕ ПРЕДСКАЗАНИЕ: За {best['lead_time']} сек "
                f"(уверенность {best['confidence']:.1%})"
            )

    def print_current_status(self):
        current_time = datetime.now()

        # Исправляем название ключа и преобразуем строку времени
        active = [
            p for p in self.predictions
            if p.get('is_confirmed') is None
               and datetime.strptime(p['predicted_time'], '%H:%M:%S').replace(year=current_time.year,
                                                                              month=current_time.month,
                                                                              day=current_time.day) > current_time
        ]

        self.logger.info("\n=== ТЕКУЩИЙ СТАТУС ПРЕДСКАЗАНИЙ ===")

        if active:
            self.logger.info("Активные предсказания:")
            for pred in sorted(active, key=lambda x: x['lead_time_sec'], reverse=True):
                # Получаем объект datetime из строки
                predicted_time = datetime.strptime(pred['predicted_time'], '%H:%M:%S').replace(
                    year=current_time.year,
                    month=current_time.month,
                    day=current_time.day
                )
                sec_left = (predicted_time - current_time).total_seconds()
                self.logger.info(
                    f"• Через {sec_left:.1f} сек (запас времени {pred['lead_time_sec']} сек, "
                    f"уверенность {pred['confidence']:.1%})"
                )
        else:
            self.logger.info("Нет активных предсказаний")

        self.logger.info("===============================\n")

    def save_history(self, filename, mode='a'):
        try:
            # Читаем существующие данные, если файл есть
            existing_data = []
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []

            # Добавляем новые предсказания
            if isinstance(existing_data, list):
                existing_data.extend(self.predictions)
            else:
                existing_data = self.predictions

            # Записываем обновленные данные
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, default=str, ensure_ascii=False)

            self.logger.info(f"История дополнена в {filename}")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения: {str(e)}")

class DataBuffer:
    def __init__(self, max_length=150):
        self.max_length = max_length
        self.buffer = deque(maxlen=max_length)
        self.last_valid_values = {}  # Хранит последние значения для всех столбцов

    def update(self, new_data: pd.DataFrame):
        """Обновляет буфер, сохраняя последние значения всех столбцов"""
        # 1. Обновляем last_valid_values для всех столбцов, которые есть в new_data
        for col in new_data.columns:
            # Если столбец полностью NaN - сохраняем предыдущее значение
            if new_data[col].isna().all():
                if col in self.last_valid_values:
                    new_data[col] = self.last_valid_values[col]
                else:
                    new_data[col] = 0  # Или другое значение по умолчанию
            else:
                # Сохраняем последнее валидное значение
                last_val = new_data[col].last_valid_index()
                if last_val is not None:
                    self.last_valid_values[col] = new_data[col].iloc[last_val]

        # 2. Добавляем новые данные в буфер
        self.buffer.extend(new_data.to_dict('records'))

        # 3. Дозаполняем буфер если нужно
        if len(self.buffer) < self.max_length and self.last_valid_values:
            missing = self.max_length - len(self.buffer)
            filler = {k: [v] * missing for k, v in self.last_valid_values.items()}
            self.buffer.extendleft(pd.DataFrame(filler).to_dict('records')[::-1])

        return pd.DataFrame(self.buffer)


model = joblib.load('random_forestFULLDATASET11sd102min.pkl')
scaler = joblib.load('scalerFULLDATASET11sd102min.pkl')


async def model_worker(queue: asyncio.Queue, collector: DataCollector):
    logger.info("Запуск обработчика предсказаний")
    buffer = deque(maxlen=150)
    prediction_logger = PredictionLogger()

    # Конфигурация тегов
    NUMERIC_TAGS = [34, 565, 566, 567, 568, 569, 570, 571, 603, 604]
    CLASS_TAGS = {600: 0, 601: 0, 602: 1}
    ALL_TAGS = NUMERIC_TAGS + list(CLASS_TAGS.keys())
    ALL_PREDICTIONS_FILE = 'all_predictions.json'
    ##########iteration_counter = 0
    try:
        while True:
            ##########iteration_counter += 1
            item = await queue.get()
            current_time = None

            try:
                # === Обработка входящих данных ===
                raw_data = item['data']

                # Создание DataFrame
                chunk_df = pd.DataFrame({
                    'timestamp': raw_data['timestamp'],
                    'archive_itemid': raw_data['archive_itemid'],
                    'value': raw_data['value']
                })

                # Конвертация времени из Windows FILETIME
                chunk_df['timestamp'] = chunk_df['timestamp'].apply(
                    lambda x: datetime.fromtimestamp((x / 10 ** 7) - 11644473600, tz=UTC)
                )
                chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'])

                # === Подготовка данных для модели ===
                # Создание pivot таблицы
                pivot_df = chunk_df.pivot_table(
                    index='timestamp',
                    columns='archive_itemid',
                    values='value',
                    aggfunc='last'
                ).resample('1s').last()

                # Добавление недостающих тегов
                for tag in ALL_TAGS:
                    if tag not in pivot_df.columns:
                        pivot_df[tag] = np.nan

                # Переименование колонок
                pivot_df = pivot_df.rename(columns=lambda x: f"value_{x}")
                processed_df = pivot_df.reset_index().rename(columns={'timestamp': 'source_time'})

                # Обновление кольцевого буфера
                buffer.extend(processed_df.to_dict('records'))
                processed_df = pd.DataFrame(buffer)

                # === Обработка пропущенных значений ===
                # Числовые теги
                for tag in NUMERIC_TAGS:
                    col = f"value_{tag}"
                    if col in processed_df.columns:
                        # Возвращаем метод интерполяции к рабочей версии
                        processed_df[col] = processed_df[col].interpolate(
                            method='linear',  # Исправлено с 'time' на 'linear'
                            limit_direction='both'
                        )

                # Категориальные теги (исправлены отступы)
                for tag, default_val in CLASS_TAGS.items():
                    col = f"value_{tag}"
                    if col in processed_df.columns:
                        processed_df[col] = processed_df[col].ffill().fillna(default_val)

                # === Выполнение предсказаний ===
                current_time = processed_df['source_time'].iloc[-1]

                try:
                    # === Выполнение предсказаний ===
                    current_time = processed_df['source_time'].iloc[-1]

                    # Всегда выполняем предсказания
                    prediction_df = prepare_prediction_steps(processed_df, steps=10)

                    if prediction_df.isnull().any().any():
                        logger.warning("Обнаружены NaN в признаках - замена средними значениями")
                        prediction_df = prediction_df.fillna(prediction_df.mean())

                    scaled = scaler.transform(prediction_df)
                    predictions = model.predict(scaled)
                    prediction_probas = model.predict_proba(scaled)

                    # Имитация предсказания на каждой 5-й итерации
                    ##########if iteration_counter % 5 == 0:
                    ##########    logger.warning("!!! ТЕСТОВОЕ ПРЕДСКАЗАНИЕ АВАРИИ !!!")
                    ##########    predictions = np.array([1] * 10)  # Все 10 шагов предсказывают аварию
                    ##########    prediction_probas = np.array([[0.0, 1.0]] * 10)  # 100% уверенность в аварии
                    # === Определение наличия аварийных предсказаний ===
                    has_emergency_prediction = any(predictions)

                    # === Логирование только при наличии предсказаний аварии ===
                    if has_emergency_prediction:
                        emergency_detected = False
                        for i, (pred, proba) in enumerate(zip(predictions, prediction_probas)):
                            if pred == 1:
                                lead_time = 10 - i
                                confidence = proba[1]

                                prediction_logger.add_prediction(
                                    timestamp=current_time,
                                    lead_time=lead_time,
                                    confidence=confidence,
                                    emergency_detected=True
                                )
                                emergency_detected = True

                        if emergency_detected:
                            prediction_logger.save_history(ALL_PREDICTIONS_FILE)

                    # === Проверка на РЕАЛЬНЫЕ аварийные события ===
                    is_emergency = (
                            processed_df['value_600'].iloc[-1] == 1 or
                            processed_df['value_601'].iloc[-1] == 1
                    )

                    if is_emergency:
                        collector.add_real_emergency()  # <-- Добавляем реальную аварию
                        logger.warning(f"!!! АВАРИЯ ОБНАРУЖЕНА В {current_time.strftime('%H:%M:%S')} !!!")
                        prediction_logger.check_predictions(current_time)
                        prediction_logger.log_best_prediction()
                        prediction_logger.save_history('emergency_predictions.json')

                    # === Логирование только при наличии предсказаний аварии ===
                    if has_emergency_prediction:
                        collector.add_predicted_emergency()  # <-- Добавляем предсказанную аварию
                        emergency_detected = False

                    # === Периодический вывод статуса (даже без аварий) ===
                    if len(buffer) % 50 == 0:
                        prediction_logger.print_current_status()
                        prediction_logger.save_history('predictions_history.json')

                    # === Визуализация результатов ===
                    # Вывод таблицы предсказаний ВСЕГДА
                    print("\n" + "=" * 80)
                    print(f"Последние предсказания ({current_time.strftime('%H:%M:%S')}):")

                    display_data = {
                        'Шаг': range(10, 0, -1),
                        'Прогноз аварии': ['Да' if p == 1 else 'Нет' for p in predictions],
                        'Уверенность (норма)': [f"{p[0]:.2%}" for p in prediction_probas],
                        'Уверенность (авария)': [f"{p[1]:.2%}" for p in prediction_probas]
                    }

                    display_df = pd.DataFrame(display_data)
                    print(display_df.to_string(index=False, justify='center'))
                    print("=" * 80 + "\n")

                    # Вывод последних 10 строк данных ВСЕГДА
                    output_df = processed_df.tail(10).copy()
                    output_df['source_time'] = output_df['source_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    print("\n" + "=" * 100)
                    print(f"Последние показания (10 из {len(processed_df)} записей):")
                    print(output_df.fillna('N/A').to_string(index=False))
                    print("=" * 100 + "\n")

                    # Сохраняем в JSON ТОЛЬКО при наличии аварийных предсказаний
                    if has_emergency_prediction:
                        prediction_logger.save_history(ALL_PREDICTIONS_FILE)
                except Exception as model_error:
                    logger.error(f"Ошибка модели: {str(model_error)}", exc_info=True)

            except Exception as processing_error:
                logger.error(f"Ошибка обработки данных: {str(processing_error)}", exc_info=True)
            finally:
                queue.task_done()

    except asyncio.CancelledError:
        logger.info("Остановка обработчика предсказаний...")
        prediction_logger.save_history('shutdown_predictions.json')
        raise

    except Exception as fatal_error:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: {str(fatal_error)}", exc_info=True)
        prediction_logger.save_history('crash_dump.json')
        raise

def prepare_prediction_steps(processed_df: pd.DataFrame, steps: int = 10) -> pd.DataFrame:
    """
    Формирует N последних записей для предсказаний на несколько шагов вперёд.
    Каждый шаг — отдельное предсказание на 1, 2, ..., N секунд до события.
    """
    df = processed_df.copy().rename(columns=lambda x: x.replace('value_', ''))
    df = df.set_index('source_time').sort_index()

    window_params = {
        '5s': {'min_periods': 5, 'fill_method': 'linear'},
        '50s': {'min_periods': 50, 'fill_method': 'ffill'},
        '2min': {'min_periods': 120, 'fill_method': 'bfill'}
    }

    for window, params in window_params.items():
        ma_col = f'34_ma_{window}'
        std_col = f'34_std_{window}'
        df[ma_col] = df['34'].rolling(window, min_periods=params['min_periods']).mean()
        df[std_col] = df['34'].rolling(window, min_periods=params['min_periods']).std()

        if params['fill_method'] == 'ffill':
            df[[ma_col, std_col]] = df[[ma_col, std_col]].ffill()
        elif params['fill_method'] == 'bfill':
            df[[ma_col, std_col]] = df[[ma_col, std_col]].bfill()
        else:
            df[[ma_col, std_col]] = df[[ma_col, std_col]].interpolate()

    df = df.reset_index()

    feature_columns = ['604', '603', '602', '571', '570', '569', '568',
                       '567', '566', '565', '34', '34_ma_5s', '34_std_5s',
                       '34_ma_50s', '34_std_50s', '34_ma_2min', '34_std_2min']

    for col in feature_columns:
        if col not in df.columns:
            df[col] = np.nan

    return df[feature_columns].tail(steps)





async def main():
    logger.info("Запуск приложения")
    db_client = PostgresClient(DB_DSN)
    collector = DataCollector(db_client)
    await collector.start()

    # Подготовка 2х потоков для обработки данных
    logger.info("Запуск потоков для обработки данных")
    workers = [asyncio.create_task(model_worker(collector.queue, collector))]
    #workers = [asyncio.create_task(model_worker(collector.queue)) for _ in range(2)]
    # запуск потоков
    await asyncio.gather(*workers)
logger = setup_logger()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Завершение работы...')
