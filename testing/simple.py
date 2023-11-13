import tensorflow as tf


# Cek apakah ada perangkat GPU yang tersedia
if tf.config.list_physical_devices('GPU'):
    print("GPU is available.")
    # Pilih perangkat GPU pertama
    device = '/GPU:0'
else:
    print("No GPU available.")
    # Jika tidak ada GPU, kita akan menggunakan CPU
    device = '/CPU:0'

# Buat sesi TensorFlow dan tentukan perangkat yang akan digunakan
with tf.device(device):
    # Buat dua matriks acak dan hitung perkaliannya
    matrix_a = tf.random.normal(shape=(1000, 1000))
    matrix_b = tf.random.normal(shape=(1000, 1000))
    result = tf.matmul(matrix_a, matrix_b)