# BigData_exercice

## Анализ результатов - Big Data

### Проверка корректности
- **Согласованность результатов**:
  - Сумма идентична (`1152895296302042263`) в обоих режимах
  - Диапазон значений:
    - Минимум: `2` (корректно, ≥ 0)
    - Максимум: `4294967282` (валидно для 32-битных unsigned)

### Соответствие требованиям
1. **create_data.py**:
   - ✅ Генерирует бинарный файл **2.0 GB**
   - ✅ Использует **32-битные big-endian** целые числа (проверено через `struct.unpack('>I')`)

2. **calc_data.py**:
   - ✅ Реализует два режима:
     - `python calc_data.py` (последовательный)
     - `python calc_data.py --parallel` (параллельный)
   - ✅ Корректно вычисляет:
     - Сумму (длинная арифметика Python)
     - Минимум/максимум

### Сравнение производительности
| Режим       | Время (сек) | Коэф. ускорения |
|-------------|------------|-----------------|
| Последовательный | 594.24     | 1.0x (база)     |
| Параллельный    | 1265.79    | 0.47x           |
