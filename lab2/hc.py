#!/usr/bin/env python3
import random
import hazelcast
import threading
import time
from datetime import datetime

###########################
# Distributed Map Demo
###########################
class DistributedMapDemo:
    def __init__(self):
        self.client = hazelcast.HazelcastClient(
            cluster_members=[
                "127.0.0.1:5701",
                "127.0.0.1:5702",
                "127.0.0.1:5703"
            ]
        )
        self.dist_map = self.client.get_map("capitals").blocking()

    def insert_entries(self):
        print("Вставляємо 1000 значень в Distributed Map 'capitals'...")
        for i in range(1, 1001):
            self.dist_map.put(str(i), f"City_{i}")
        print("Значення записані.")

    def shutdown(self):
        self.client.shutdown()

###########################
# Concurrent Increment Demo
###########################
class ConcurrentIncrementDemo:
    """    
    increment_type:
       0 - no lock
       1 - optimistic
       2 - pessimistic
       3 - atomic
    """
    def __init__(self, increment_type: int, count: int = 10_000, thread_num: int = 3):
        self.increment_type = increment_type
        self.count = count
        self.thread_num = thread_num
        self.client = hazelcast.HazelcastClient(
            cluster_members=[
                "127.0.0.1:5701",
                "127.0.0.1:5702",
                "127.0.0.1:5703"
            ]
        )
        # Если используется атомик (тип 3), работаем через CP-субсистему
        if self.increment_type == 3:
            self.counter = self.client.cp_subsystem.get_atomic_long("key").blocking()
        else:
            # Для остальных типов используем распределённую карту
            self.map = self.client.get_map("counter_map").blocking()

    def reset_key(self):
        if self.increment_type == 3:
            self.counter.set(0)
        else:
            self.map.put("key", 0)

    def get_value(self):
        if self.increment_type == 3:
            return self.counter.get()
        else:
            val = self.map.get("key")
            return val if val is not None else 0

    def increment_counter(self):
        # no lock
        for _ in range(self.count):
            current = self.map.get("key") or 0
            self.map.put("key", current + 1)

    def optimistic_increment(self):
        for _ in range(self.count):
            while True:
                old_value = self.map.get("key") or 0
                new_value = old_value + 1
                # Метод replace_if_same возвращает True, если замена удалась
                if self.map.replace_if_same("key", old_value, new_value):
                    break

    def pessimistic_increment(self):
        for _ in range(self.count):
            self.map.lock("key")
            try:
                current = self.map.get("key") or 0
                self.map.put("key", current + 1)
            finally:
                self.map.unlock("key")

    def atomic_increment(self):
        for _ in range(self.count):
            self.counter.increment_and_get()

    def run_test(self):
        self.reset_key()
        if self.increment_type == 0:
            func = self.increment_counter
            test_name = "No lock"
        elif self.increment_type == 1:
            func = self.optimistic_increment
            test_name = "Optimistic lock"
        elif self.increment_type == 2:
            func = self.pessimistic_increment
            test_name = "Pessimistic lock"
        elif self.increment_type == 3:
            func = self.atomic_increment
            test_name = "Atomic"
        else:
            print("Unknown increment type")
            return

        print(f"\nStart test: {test_name}")
        start_time = time.time()
        threads = []
        for i in range(self.thread_num):
            thread = threading.Thread(target=func, name=f"IncThread-{i+1}")
            thread.start()
            threads.append(thread)
        # Ожидаем завершения всех потоков
        for thread in threads:
            thread.join()
        elapsed = time.time() - start_time
        final_value = self.get_value()
        print(f"Результат теста '{test_name}': фінальне значення = {final_value}, час виконання = {elapsed:.2f} секунд")
        if self.increment_type == 0 and final_value != self.thread_num * self.count:
            print("Очікувана втрата даних")
        elif self.increment_type in (1, 2, 3):
            if final_value == self.thread_num * self.count:
                print("Дані коректні.")
            else:
                print("Помилка, дані не співпадають з очікуваними.")

    def shutdown(self):
        self.client.shutdown()

###########################
# Bounded Queue Demo
###########################
class BoundedQueueDemo:
    """
    Демонстрация работы ограниченной очереди (Bounded Queue) на основе распределённой очереди.
    Для корректной работы ограниченной очереди необходимо на стороне сервера задать максимальный размер очереди (например, 10).
    """
    def __init__(self):
        self.client = hazelcast.HazelcastClient(
            cluster_members=[
                "127.0.0.1:5701",
                "127.0.0.1:5702",
                "127.0.0.1:5703"
            ]
        )
        # Получаем распределённую очередь. Конфигурация лимита (bound) должна быть прописана в конфигурационном файле Hazelcast.
        self.queue = self.client.get_queue("bounded_queue").blocking()

    def producer(self):
        print("Продюсер: починаємо вставляти значення от 1 до 100...")
        for i in range(1, 101):
            print(f"Продюсер намагається додати: {i}")
            # Используем offer с таймаутом, чтобы не блокироваться бесконечно, если очередь заполнена
            try:
                added = self.queue.offer(i, timeout=2)
                if added:
                    print(f"Продюсер додав: {i}")
                else:
                    print(f"Продюсер не зміг додати {i} – черга заповнена")
            except Exception as e:
                print(f"Продюсер: помилка при додаванні {i}: {e}")
            time.sleep(0.1)
        print("Продюсер: завершив вставку значень.")

    def consumer(self, consumer_id):
        print(f"Консюмер {consumer_id}: запускає читання з черги...")
        while True:
            try:
                # poll с таймаутом: если за 5 секунд ничего не придёт, считаем, что очередь пуста и выходим
                item = self.queue.poll(timeout=5)
                if item is None:
                    print(f"Консюмер {consumer_id}: черга порожня, завершую роботу.")
                    break
                print(f"Консюмер {consumer_id} отримав: {item}")
            except Exception as e:
                print(f"Консюмер {consumer_id}: помилка: {e}")
            time.sleep(0.2)

    def run_demo(self):
        # Запуск продюсера и двух консюмеров
        producer_thread = threading.Thread(target=self.producer, name="ProducerThread")
        consumer_thread1 = threading.Thread(target=self.consumer, args=(1,), name="ConsumerThread-1")
        consumer_thread2 = threading.Thread(target=self.consumer, args=(2,), name="ConsumerThread-2")
        producer_thread.start()
        consumer_thread1.start()
        consumer_thread2.start()
        producer_thread.join()
        consumer_thread1.join()
        consumer_thread2.join()
        print("Демонстрація Bounded Queue завершена.")

    def shutdown(self):
        self.client.shutdown()

###########################
# Main
###########################
def main():
    print("=== Hazelcast Lab Demo ===\n")
    
    # # 1. Distributed Map Demo
    # print(">>> 1. Демонстрація Distributed Map")
    # map_demo = DistributedMapDemo()
    # map_demo.insert_entries()
    # map_demo.shutdown()
    
    # # 2. Concurrent Increment Demo
    # print("\n>>> 2. Демонстрація конкурентного інкремента в Distributed Map")
    # # Запускаем тесты для:
    # # 0 - Без блокировок
    # # 1 - Оптимистичная блокировка
    # # 2 - Песимистичная блокировка
    # for inc_type, test_name in [(0, "Без блокировок"), (1, "Оптимистичная блокировка"), (2, "Песимистичная блокировка")]:
    #     demo = ConcurrentIncrementDemo(increment_type=inc_type, count=10_000, thread_num=3)
    #     demo.run_test()
    #     demo.shutdown()

    # 3. Bounded Queue Demo
    print("\n>>> 3. Демонстрація роботи Bounded Queue")
    queue_demo = BoundedQueueDemo()
    queue_demo.run_demo()
    queue_demo.shutdown()

    print("\n=== Демонстрація завершена ===")

if __name__ == "__main__":
    main()
