import multiprocessing as mp
import random
import time
from datetime import datetime

def get_timestamp():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def log_message(source, message):
    print(f"[{get_timestamp()}] [{source}] {message}")

def is_sorted(arr):
    return all(arr[i] <= arr[i + 1] for i in range(len(arr) - 1))

def quicksort_partial(arr, low, high, start_time, time_limit):
    if time.time() - start_time >= time_limit:
        return False
    
    if low < high:
        pivot = partition(arr, low, high)
        if not quicksort_partial(arr, low, pivot - 1, start_time, time_limit):
            return False
        if not quicksort_partial(arr, pivot + 1, high, start_time, time_limit):
            return False
    return True

def partition(arr, low, high):
    pivot = arr[high]
    i = low - 1
    for j in range(low, high):
        if arr[j] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i + 1], arr[high] = arr[high], arr[i + 1]
    return i + 1

def mergesort_partial(arr, low, high, start_time, time_limit):
    if time.time() - start_time >= time_limit:
        return False
        
    if low < high:
        mid = (low + high) // 2
        if not mergesort_partial(arr, low, mid, start_time, time_limit):
            return False
        if not mergesort_partial(arr, mid + 1, high, start_time, time_limit):
            return False
        merge(arr, low, mid, high)
    return True

def merge(arr, low, mid, high):
    left = arr[low:mid + 1]
    right = arr[mid + 1:high + 1]
    i = j = 0
    k = low
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            arr[k] = left[i]
            i += 1
        else:
            arr[k] = right[j]
            j += 1
        k += 1
    while i < len(left):
        arr[k] = left[i]
        i += 1
        k += 1
    while j < len(right):
        arr[k] = right[j]
        j += 1
        k += 1

def heapsort_partial(arr, start_time, time_limit):
    n = len(arr)
    
    for i in range(n // 2 - 1, -1, -1):
        if time.time() - start_time >= time_limit:
            return False
        heapify(arr, n, i)
    
    for i in range(n - 1, 0, -1):
        if time.time() - start_time >= time_limit:
            return False
        arr[i], arr[0] = arr[0], arr[i]
        heapify(arr, i, 0)
    return True

def heapify(arr, n, i):
    largest = i
    left = 2 * i + 1
    right = 2 * i + 2
    
    if left < n and arr[left] > arr[largest]:
        largest = left
    if right < n and arr[right] > arr[largest]:
        largest = right
        
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]
        heapify(arr, n, largest)

def worker_function(pipe, algorithm, time_limit, worker_id):
    conn = pipe
    worker_name = f"Worker {worker_id}"
    iterations = 0
    
    while True:
        if conn.poll():
            data = conn.recv()
            if data == "STOP":
                break

            vector, low, high, start_time = data
            iterations += 1
            
            log_message(worker_name, f"Iteración {iterations}")
            
            if is_sorted(vector):
                log_message(worker_name, "Vector ya está ordenado - No se requiere procesamiento")
                conn.send(("DONE", vector, time.time() - start_time))
                continue

            try:
                completed = False
                current_time = time.time()
                
                if algorithm == "mergesort":
                    completed = mergesort_partial(vector, low, high, current_time, time_limit)
                elif algorithm == "quicksort":
                    completed = quicksort_partial(vector, low, high, current_time, time_limit)
                elif algorithm == "heapsort":
                    completed = heapsort_partial(vector, current_time, time_limit)
                
                elapsed_time = time.time() - current_time
                
                if completed and is_sorted(vector):
                    log_message(worker_name, f"¡Ordenamiento completado en {elapsed_time:.2f}s!")
                    log_message(worker_name, f"Primeros elementos ordenados: {vector[:10]}...")
                    conn.send(("DONE", vector, elapsed_time))
                else:
                    log_message(worker_name, f"Tiempo límite excedido ({time_limit}s)")
                    log_message(worker_name, f"Progreso actual: {vector[:5]}...")
                    conn.send(("CONTINUE", vector, low, high, elapsed_time))
            
            except Exception as e:
                log_message(worker_name, f"Error durante el ordenamiento: {str(e)}")
                conn.send(("ERROR", str(e)))

def client():
    log_message("Cliente", "Iniciando programa de ordenamiento distribuido")
    
    n = int(input("Tamaño del vector: "))
    
    algorithm = input("Algoritmo (mergesort/quicksort/heapsort): ").lower()
    
    time_limit = float(input("Tiempo límite para cada worker (segundos): "))

    log_message("Cliente", "Generando vector aleatorio...")
    vector = [random.randint(0, 100000) for _ in range(n)]
    log_message("Cliente", f"Vector generado. Primeros elementos: {vector[:10]}...")

    log_message("Cliente", "Iniciando workers...")
    pipe_worker_0, pipe_client_0 = mp.Pipe()
    pipe_worker_1, pipe_client_1 = mp.Pipe()

    worker_0 = mp.Process(target=worker_function, args=(pipe_worker_0, algorithm, time_limit, 0))
    worker_1 = mp.Process(target=worker_function, args=(pipe_worker_1, algorithm, time_limit, 1))
    
    start_time = time.time()
    worker_0.start()
    worker_1.start()

    log_message("Cliente", "Enviando vector inicial al Worker 0")
    pipe_client_0.send((vector, 0, len(vector) - 1, time.time()))
    
    transfers = 0
    total_time = 0
    
    while True:
        if pipe_client_0.poll(1):
            data = pipe_client_0.recv()
            status = data[0]
            
            if status == "DONE":
                _, sorted_vector, worker_time = data
                total_time = time.time() - start_time
                log_message("Cliente", f"¡Ordenamiento completado por Worker 0!")
                log_message("Cliente", f"Tiempo total: {total_time:.2f}s")
                log_message("Cliente", f"Número de transferencias: {transfers}")
                log_message("Cliente", f"Vector ordenado (primeros elementos): {sorted_vector[:10]}...")
                break
            
            elif status == "CONTINUE":
                _, vector, low, high, worker_time = data
                transfers += 1
                log_message("Cliente", f"Transferencia #{transfers}: Worker 0 → Worker 1")
                pipe_client_1.send((vector, low, high, time.time()))

        if pipe_client_1.poll(1):
            data = pipe_client_1.recv()
            status = data[0]
            
            if status == "DONE":
                _, sorted_vector, worker_time = data
                total_time = time.time() - start_time
                log_message("Cliente", f"¡Ordenamiento completado por Worker 1!")
                log_message("Cliente", f"Tiempo total: {total_time:.2f}s")
                log_message("Cliente", f"Número de transferencias: {transfers}")
                log_message("Cliente", f"Vector ordenado (primeros elementos): {sorted_vector[:10]}...")
                break
            
            elif status == "CONTINUE":
                _, vector, low, high, worker_time = data
                transfers += 1
                log_message("Cliente", f"Transferencia #{transfers}: Worker 1 → Worker 0")
                pipe_client_0.send((vector, low, high, time.time()))


    log_message("Cliente", "Enviando señal de terminación a los workers")
    pipe_client_0.send("STOP")
    pipe_client_1.send("STOP")
    
    worker_0.join()
    worker_1.join()
    log_message("Cliente", "Programa finalizado")

if __name__ == "__main__":
    client()



