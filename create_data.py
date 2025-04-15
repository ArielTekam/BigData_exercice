import random
import os

def generate_binary_file(filename='data.bin', size_gb=2):
    """Генерирует бинарный файл из 32-битных беззнаковых целых чисел"""
    size_bytes = size_gb * 1024**3  # Размер в байтах
    num_ints = size_bytes // 4  # Количество целых чисел
    
    with open(filename, 'wb') as f:
        for _ in range(num_ints):
            # Генерация числа (0 ≤ num < 2^32)
            num = random.randint(0, 2**32 - 1)
            # Запись в бинарном формате (big-endian)
            f.write(num.to_bytes(4, byteorder='big'))
    
    print(f"Файл {filename} создан. Размер: {os.path.getsize(filename) / 1024**3:.2f} GB")

if __name__ == '__main__':
    generate_binary_file()