import json
from kafka import KafkaConsumer
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import time

# ===== СТРУКТУРЫ ДАННЫХ =====
user_behavior = defaultdict(list)
session_data = defaultdict(dict)
hourly_activity = defaultdict(int)
conversion_funnel = Counter()
revenue_data = []
product_performance = defaultdict(lambda: {
    'views': 0,
    'cart_adds': 0,
    'purchases': 0,
    'revenue': 0
})

recent_events = []
event_timestamps = defaultdict(list)

# ===== ИНИЦИАЛИЗАЦИЯ KAFKA CONSUMER =====
consumer = KafkaConsumer(
    'user-actions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='demo-analytics-group'
)

print("Запуск потребления сообщений...")
print("Нажмите Ctrl+C для остановки и просмотра аналитики")

message_count = 0
start_time = time.time()

# ===== ФУНКЦИИ ДЛЯ РАСЧЁТА МЕТРИК =====

def calculate_conversion_rates():
    """Рассчитывает конверсию между этапами воронки продаж"""
    views = conversion_funnel.get('view_product', 0)
    cart_adds = conversion_funnel.get('add_to_cart', 0)
    purchases = conversion_funnel.get('purchase', 0)
    
    view_to_cart = (cart_adds / views * 100) if views > 0 else 0
    cart_to_purchase = (purchases / cart_adds * 100) if cart_adds > 0 else 0
    view_to_purchase = (purchases / views * 100) if views > 0 else 0
    
    return {
        'view_to_cart_rate': round(view_to_cart, 2),
        'cart_to_purchase_rate': round(cart_to_purchase, 2),
        'view_to_purchase_rate': round(view_to_purchase, 2),
        'total_views': views,
        'total_cart_adds': cart_adds,
        'total_purchases': purchases
    }


def calculate_average_session_value():
    """Считает средний чек сессии"""
    if not revenue_data:
        return {
            'average_session_value': 0,
            'total_revenue': 0,
            'sessions_with_purchase': 0
        }
    
    total_revenue = sum(revenue_data)
    sessions_with_purchase = len([u for u, actions in user_behavior.items() 
                                   if 'purchase' in actions])
    avg_value = total_revenue / sessions_with_purchase if sessions_with_purchase > 0 else 0
    
    return {
        'average_session_value': round(avg_value, 2),
        'total_revenue': round(total_revenue, 2),
        'sessions_with_purchase': sessions_with_purchase
    }


def find_top_customers(top_n=5):
    """Находит топ-N самых активных пользователей"""
    if not user_behavior:
        return []
    
    user_activity = [(user, len(actions)) for user, actions in user_behavior.items()]
    user_activity.sort(key=lambda x: x[1], reverse=True)
    
    return [
        {'user_id': user, 'action_count': count}
        for user, count in user_activity[:top_n]
    ]


def detect_abandonment_sessions():
    """Обнаруживает сессии с брошенными корзинами"""
    abandoned = []
    
    for user_id, actions in user_behavior.items():
        if 'add_to_cart' in actions and 'purchase' not in actions:
            abandoned.append({
                'user_id': user_id,
                'actions_count': len(actions),
                'last_action': actions[-1] if actions else None
            })
    
    return {
        'abandoned_sessions': abandoned,
        'abandonment_count': len(abandoned),
        'abandonment_rate': round(len(abandoned) / len(user_behavior) * 100, 2) if user_behavior else 0
    }


def get_top_products_by_revenue(top_n=10):
    """Возвращает топ продуктов по выручке"""
    products = []
    for product, stats in product_performance.items():
        products.append({
            'product': product,
            'revenue': stats['revenue'],
            'views': stats['views'],
            'purchases': stats['purchases']
        })
    
    products.sort(key=lambda x: x['revenue'], reverse=True)
    return products[:top_n]


# ===== ФУНКЦИЯ ПРОВЕРКИ АНОМАЛИЙ =====

def check_for_alerts(recent_events_list):
    """Проверяет последние события на подозрительную активность"""
    alerts = []
    
    if not recent_events_list:
        return alerts
    
    logout_count = sum(1 for e in recent_events_list if e.get('action') == 'logout')
    logout_percentage = (logout_count / len(recent_events_list) * 100) if recent_events_list else 0
    
    if logout_percentage > 50:
        alerts.append({
            'type': 'HIGH_LOGOUT_RATE',
            'severity': 'WARNING',
            'message': 'Слишком много logout: {:.1f}%'.format(logout_percentage)
        })
    
    abandonment = detect_abandonment_sessions()
    if abandonment['abandonment_rate'] > 70:
        alerts.append({
            'type': 'HIGH_CART_ABANDONMENT',
            'severity': 'CRITICAL',
            'message': 'Высокий уровень отказов корзины: {}%'.format(
                abandonment['abandonment_rate'])
        })
    
    current_time = time.time()
    for user_id, timestamps in event_timestamps.items():
        recent_timestamps = [t for t in timestamps if current_time - t < 60]
        event_timestamps[user_id] = recent_timestamps
        
        if len(recent_timestamps) > 10:
            alerts.append({
                'type': 'SUSPICIOUS_USER_ACTIVITY',
                'severity': 'WARNING',
                'message': 'Пользователь {} сделал {} действий за минуту'.format(
                    user_id, len(recent_timestamps))
            })
    
    last_50 = recent_events_list[-50:] if len(recent_events_list) >= 50 else recent_events_list
    purchases_in_last_50 = sum(1 for e in last_50 if e.get('action') == 'purchase')
    
    if len(last_50) >= 50 and purchases_in_last_50 == 0:
        alerts.append({
            'type': 'NO_PURCHASES_ALERT',
            'severity': 'WARNING',
            'message': 'Нет покупок в последних {} событиях'.format(len(last_50))
        })
    
    return alerts

# ===== ФУНКЦИЯ СОЗДАНИЯ ВИЗУАЛИЗАЦИЙ =====

def create_analytics_dashboard():
    """Создаёт дашборд с 6 графиками аналитики"""
    fig = plt.figure(figsize=(16, 12))
    fig.suptitle('Kafka Analytics Dashboard', fontsize=16, fontweight='bold')
    
    # График 1: Воронка конверсий
    plt.subplot(2, 3, 1)
    conv_rates = calculate_conversion_rates()
    funnel_labels = ['Просмотры', 'Корзина', 'Покупки']
    funnel_values = [conv_rates['total_views'], conv_rates['total_cart_adds'], conv_rates['total_purchases']]
    funnel_colors = ['#3498db', '#e67e22', '#2ecc71']
    
    y_pos = np.arange(len(funnel_labels))
    plt.barh(y_pos, funnel_values, color=funnel_colors, edgecolor='black')
    plt.yticks(y_pos, funnel_labels)
    plt.xlabel('Количество')
    plt.title('Воронка конверсий')
    plt.grid(axis='x', alpha=0.3)
    
    for i, v in enumerate(funnel_values):
        plt.text(v + 1, i, str(v), va='center')
    
    # График 2: Топ продуктов по выручке
    plt.subplot(2, 3, 2)
    top_products = get_top_products_by_revenue(10)
    
    if top_products:
        products_names = [p['product'] for p in top_products]
        products_revenue = [p['revenue'] for p in top_products]
        
        plt.bar(range(len(products_names)), products_revenue, color='#9b59b6', edgecolor='black')
        plt.xticks(range(len(products_names)), products_names, rotation=45, ha='right')
        plt.xlabel('Продукт')
        plt.ylabel('Выручка (руб.)')
        plt.title('Топ продуктов по выручке')
        plt.grid(axis='y', alpha=0.3)
    else:
        plt.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=plt.gca().transAxes)
        plt.title('Топ продуктов по выручке')
    
    # График 3: Активность пользователей
    plt.subplot(2, 3, 3)
    
    if user_behavior:
        user_ids = list(user_behavior.keys())
        action_counts = [len(actions) for actions in user_behavior.values()]
        
        user_revenue = defaultdict(float)
        for event in recent_events:
            if event.get('price') and event.get('action') == 'purchase':
                user_revenue[event['user_id']] += event.get('price', 0)
        
        bubble_sizes = [max(user_revenue.get(uid, 0) / 100, 10) for uid in user_ids]
        
        scatter = plt.scatter(user_ids, action_counts, s=bubble_sizes, 
                             c=bubble_sizes, cmap='viridis', alpha=0.6, edgecolors='black')
        plt.xlabel('User ID')
        plt.ylabel('Количество действий')
        plt.title('Активность пользователей (размер = выручка)')
        plt.grid(alpha=0.3)
        plt.colorbar(scatter, label='Выручка')
    else:
        plt.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=plt.gca().transAxes)
        plt.title('Активность пользователей')
    
    # График 4: Распределение длины сессий
    plt.subplot(2, 3, 4)
    
    if user_behavior:
        session_lengths = [len(actions) for actions in user_behavior.values()]
        
        plt.hist(session_lengths, bins=10, color='#e74c3c', edgecolor='black', orientation='horizontal')
        plt.xlabel('Количество пользователей')
        plt.ylabel('Длина сессии (действий)')
        plt.title('Распределение длины сессий')
        plt.grid(axis='x', alpha=0.3)
    else:
        plt.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=plt.gca().transAxes)
        plt.title('Распределение длины сессий')
    
    # График 5: Активность по времени
    plt.subplot(2, 3, 5)
    
    if hourly_activity:
        hours = sorted(hourly_activity.keys())
        activity_values = [hourly_activity[h] for h in hours]
        
        plt.plot(hours, activity_values, marker='o', linewidth=2, color='#1abc9c', markersize=8)
        plt.xlabel('Час суток')
        plt.ylabel('Количество событий')
        plt.title('Активность по времени')
        plt.grid(alpha=0.3)
        plt.xticks(range(24))
    else:
        plt.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=plt.gca().transAxes)
        plt.title('Активность по времени')
    
    # График 6: Соотношение типов действий
    plt.subplot(2, 3, 6)
    
    if conversion_funnel:
        action_labels = list(conversion_funnel.keys())
        action_values = list(conversion_funnel.values())
        pie_colors = ['#3498db', '#e67e22', '#2ecc71', '#9b59b6', '#e74c3c', '#1abc9c']
        
        plt.pie(action_values, labels=action_labels, autopct='%1.1f%%', 
               colors=pie_colors[:len(action_labels)], startangle=90)
        plt.title('Соотношение типов действий')
    else:
        plt.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=plt.gca().transAxes)
        plt.title('Соотношение типов действий')
    
    plt.tight_layout()
    plt.savefig('kafka_analytics_dashboard.png', dpi=150, bbox_inches='tight')
    print("Дашборд сохранён в kafka_analytics_dashboard.png")
    plt.close()


# ===== ФУНКЦИЯ СОЗДАНИЯ ТЕКСТОВОГО ОТЧЁТА =====

def generate_text_report():
    """Формирует текстовый отчёт и выводит в консоль и файл"""
    conv_rates = calculate_conversion_rates()
    avg_session = calculate_average_session_value()
    top_products = get_top_products_by_revenue(1)
    alerts = check_for_alerts(recent_events)
    
    report_lines = []
    report_lines.append("=" * 50)
    report_lines.append("ОТЧЁТ ПО АНАЛИТИКЕ KAFKA")
    report_lines.append("=" * 50)
    report_lines.append("Обработано событий: {}".format(message_count))
    report_lines.append("Уникальных пользователей: {}".format(len(user_behavior)))
    report_lines.append("Конверсия покупок: {}%".format(conv_rates['view_to_purchase_rate']))
    report_lines.append("Общая выручка: {} руб.".format(avg_session['total_revenue']))
    
    if top_products:
        report_lines.append("Топ продукт: {} ({} руб.)".format(
            top_products[0]['product'], top_products[0]['revenue']))
    else:
        report_lines.append("Топ продукт: нет данных")
    
    report_lines.append("Оповещения: {} подозрительных сессии".format(len(alerts)))
    report_lines.append("=" * 50)
    
    report_text = "\n".join(report_lines)
    print("\n" + report_text)
    
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = "report_{}.txt".format(current_datetime)
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    print("Отчёт сохранён в {}".format(filename))
    return report_text

# ===== ФУНКЦИЯ СОХРАНЕНИЯ CSV =====

def export_session_data_to_csv():
    """Сохраняет данные о сессиях пользователей в CSV файл"""
    session_records = []
    
    for user_id, actions in user_behavior.items():
        if actions:
            session_records.append({
                'user_id': user_id,
                'total_actions': len(actions),
                'actions': '|'.join(actions),
                'first_action': actions[0],
                'last_action': actions[-1],
                'has_purchase': 'purchase' in actions,
                'has_cart_add': 'add_to_cart' in actions,
                'timestamp': datetime.now().isoformat()
            })
    
    if session_records:
        df = pd.DataFrame(session_records)
        df.to_csv('user_sessions.csv', index=False, encoding='utf-8-sig')
        print("Сохранено {} сессий в user_sessions.csv".format(len(session_records)))
        return True
    else:
        print("Нет данных для сохранения в CSV")
        return False
    
# ===== ФУНКЦИЯ СОХРАНЕНИЯ JSON =====

def save_real_time_metrics():
    """Сохраняет все текущие метрики в JSON файл"""
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'message_count': message_count,
        'uptime_seconds': round(time.time() - start_time, 2),
        'conversion_rates': calculate_conversion_rates(),
        'average_session_value': calculate_average_session_value(),
        'top_customers': find_top_customers(5),
        'abandonment_sessions': detect_abandonment_sessions(),
        'hourly_activity': dict(hourly_activity),
        'product_performance': {k: dict(v) for k, v in product_performance.items()},
        'total_users': len(user_behavior),
        'total_revenue': sum(revenue_data)
    }
    
    with open('metrics_snapshot.json', 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    
    print("Метрики сохранены в metrics_snapshot.json")
    return metrics
# ===== ОСНОВНОЙ ЦИКЛ ПОТРЕБЛЕНИЯ =====

try:
    for message in consumer:
        event = message.value
        message_count += 1
        current_time = time.time()
        
        user_behavior[event['user_id']].append(event['action'])
        
        session_data[event['user_id']].update({
            'last_action': event['action'],
            'last_action_time': current_time,
            'action_count': len(user_behavior[event['user_id']])
        })
        
        hour = datetime.fromtimestamp(current_time).hour
        hourly_activity[hour] += 1
        
        conversion_funnel[event['action']] += 1
        
        if event.get('price'):
            revenue_data.append(event['price'])
        
        product = event.get('product', 'unknown')
        if event['action'] == 'view_product':
            product_performance[product]['views'] += 1
        elif event['action'] == 'add_to_cart':
            product_performance[product]['cart_adds'] += 1
        elif event['action'] == 'purchase':
            product_performance[product]['purchases'] += 1
            product_performance[product]['revenue'] += event.get('price', 0)
        
        recent_events.append(event)
        if len(recent_events) > 100:
            recent_events.pop(0)
        
        event_timestamps[event['user_id']].append(current_time)
        
        print("Сообщение {}: {} - {} - User {}".format(
            message_count, 
            event['action'], 
            event.get('product', 'N/A'), 
            event['user_id']
        ))

except KeyboardInterrupt:
    print("\n\nОстановлено после {} сообщений".format(message_count))
    print("Время работы: {} секунд".format(round(time.time() - start_time, 2)))
    
    print("\n" + "=" * 60)
    print("ФИНАЛЬНАЯ АНАЛИТИКА ПО ВСЕЙ ВЫБОРКЕ")
    print("=" * 60)
    
    print("\nВсего сообщений: {}".format(message_count))
    print("Всего пользователей: {}".format(len(user_behavior)))
    print("Общая выручка: {} руб.".format(sum(revenue_data)))
    
    conv_rates = calculate_conversion_rates()
    print("\nИтоговые коэффициенты конверсии:")
    print("  Просмотр -> Корзина: {}%".format(conv_rates['view_to_cart_rate']))
    print("  Корзина -> Покупка: {}%".format(conv_rates['cart_to_purchase_rate']))
    print("  Просмотр -> Покупка: {}%".format(conv_rates['view_to_purchase_rate']))
    
    print("\nСохранение финальных данных...")
    
    create_analytics_dashboard()
    generate_text_report()
    export_session_data_to_csv()      
    save_real_time_metrics()  
    print("\nВсе данные успешно сохранены!")