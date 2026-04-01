import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['laptop', 'smartphone', 'tablet', 'headphones', 'keyboard', 'mouse']
# Фиксированные цены для реалистичности (чтобы цена при покупке совпадала с ценой в корзине)
BASE_PRICES = {
    'laptop': 1000,
    'smartphone': 800,
    'tablet': 500,
    'headphones': 150,
    'keyboard': 100,
    'mouse': 50
}


class SessionState:
    def __init__(self, session_id):
        self.session_id = session_id
        self.user_id = random.randint(1, 100)
        self.viewed_products = set()
        self.cart = {}  # {product: price}
        self.is_active = True

    def view_product(self, product):
        self.viewed_products.add(product)
        return product

    def add_to_cart(self, product):
        if product in self.viewed_products:
            # Небольшая вариация цены для реалистичности
            price = random.uniform(0.95, 1.05) * BASE_PRICES[product]
            self.cart[product] = price
            return product, price
        return None, None

    def purchase(self):
        """
        Покупка ТОЛЬКО ОДНОГО продукта из корзины.
        Остальные товары остаются в корзине для будущих покупок.
        """
        if not self.cart:
            return None, None

        # Выбираем случайный товар из корзины для покупки
        product_to_buy = random.choice(list(self.cart.keys()))
        price = self.cart[product_to_buy]

        # Удаляем купленный товар из корзины
        del self.cart[product_to_buy]

        return product_to_buy, price

    def logout(self):
        self.is_active = False
        self.viewed_products.clear()
        self.cart.clear()


class SessionManager:
    def __init__(self):
        self.sessions = {}

    def get_or_create_session(self):
        # 85% шанс продолжить существующую сессию, 15% начать новую
        if self.sessions and random.random() > 0.15:
            session_id = random.choice(list(self.sessions.keys()))
            session = self.sessions[session_id]
            if not session.is_active:
                session = self.create_new_session(session_id)
            return session
        else:
            return self.create_new_session()

    def create_new_session(self, session_id=None):
        if session_id is None:
            session_id = f"session_{random.randint(1000, 9999)}"
        self.sessions[session_id] = SessionState(session_id)
        return self.sessions[session_id]

    def generate_next_action(self, session: SessionState) -> dict:
        action = None

        # Логика выбора действия на основе состояния (Воронка продаж)
        possible_actions = ['search', 'view_product']

        if session.viewed_products:
            possible_actions.append('add_to_cart')

        if session.cart:
            possible_actions.append('purchase')

        # В любой момент можно выйти
        possible_actions.append('logout')

        # Взвешенный случайный выбор
        weights = []
        for act in possible_actions:
            if act == 'purchase':
                weights.append(3)
            elif act == 'add_to_cart':
                weights.append(3)
            elif act == 'logout':
                weights.append(1)
            else:
                weights.append(1)

        action = random.choices(possible_actions, weights=weights)[0]

        # Формирование события
        event = {
            "user_id": session.user_id,
            "session_id": session.session_id,
            "action": action,
            "timestamp": datetime.now().isoformat()
        }

        if action == 'search':
            event["query"] = random.choice(products)

        elif action == 'view_product':
            product = random.choice(products)
            session.view_product(product)
            event["product"] = product

        elif action == 'add_to_cart':
            valid_products = list(session.viewed_products)
            if valid_products:
                product = random.choice(valid_products)
                prod, price = session.add_to_cart(product)
                event["product"] = prod
                event["price"] = price
            else:
                # Fallback
                action = 'view_product'
                product = random.choice(products)
                session.view_product(product)
                event["action"] = action
                event["product"] = product

        elif action == 'purchase':
            # Теперь purchase возвращает только 1 продукт
            if session.cart:
                product, price = session.purchase()
                event["product"] = product  # Единообразное поле (не products)
                event["price"] = price  # Единообразное поле (не total_price)
            else:
                # Fallback
                action = 'view_product'
                product = random.choice(products)
                session.view_product(product)
                event["action"] = action
                event["product"] = product

        elif action == 'logout':
            session.logout()
            if session.session_id in self.sessions:
                del self.sessions[session.session_id]

        return event


# Инициализация
manager = SessionManager()

print("Starting realistic user simulation...")
print("Press Ctrl+C to stop")

try:
    i = 0
    while True:
        session = manager.get_or_create_session()
        event = manager.generate_next_action(session)

        producer.send('user-actions', value=event)
        price_display = f"{event.get('price'):.2f}" if event.get('price') else 'N/A'
        print(f"{i + 1:<6} | {event['user_id']:<6} | {event['session_id']:<15} | {event['action']:<15} | "
              f"{(event.get('product') or 'N/A'):<12} | {price_display:<10} | {event['timestamp']:<26}")

        time.sleep(random.uniform(0.5, 1.0))
        i += 1

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.flush()
    producer.close()
    print("Producer stopped!")