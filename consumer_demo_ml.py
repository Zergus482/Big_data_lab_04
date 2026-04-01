import json
from kafka import KafkaConsumer
import numpy as np

# Для демонстрации используем простую логику вместо реальной ML-библиотеки

consumer = KafkaConsumer(
    'user-actions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',  # Читаем только новые сообщения
    group_id='ml-pipeline-group'
)


def process_for_ml(event_data):
    """Преобразуем событие пользователя в признаки для ML модели"""
    features = []

    # Признак 1: цена (если есть)
    price = event_data.get('price', 0)
    features.append(price if price else 100)  # средняя цена по умолчанию

    # Признак 2: категория товара (хешируем название)
    product = event_data.get('product', 'unknown')
    category_feature = hash(product) % 100 if product else 0
    features.append(category_feature)

    # Признак 3: сегмент пользователя (по user_id)
    user_segment = event_data['user_id'] % 10
    features.append(user_segment)

    # Признак 4: тип действия
    action_mapping = {
        'view_product': 1,
        'add_to_cart': 2,
        'purchase': 3,
        'search': 4,
        'logout': 5
    }
    action_feature = action_mapping.get(event_data['action'], 0)
    features.append(action_feature)

    return np.array(features).reshape(1, -1)


def simple_recommendation_model(features):
    """Простая имитация ML-модели для рекомендаций"""
    # В реальности здесь была бы обученная модель
    score = np.sum(features) % 100

    if score > 70:
        return "premium_products"
    elif score > 40:
        return "popular_products"
    else:
        return "budget_products"


print("ML Pipeline started. Processing events for recommendations...")

for message in consumer:
    event = message.value

    # Преобразуем событие в признаки
    features = process_for_ml(event)

    # Получаем рекомендацию от "модели"
    recommendation = simple_recommendation_model(features)

    print(f"User {event['user_id']} performed '{event['action']}' → Recommend: {recommendation}")
    print(f"  Features used: {features.flatten()}")
    print("-" * 60)