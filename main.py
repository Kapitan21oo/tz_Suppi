import numpy as np
import pandas as pd

# Установка опции для отображения всех столбцов без ограничения
pd.set_option('display.max_columns', None)

# Путь к файлу с данными
file = r'C:\Users\Gurki\PycharmProjects\tz_Suppi\trial_task.json'

# Чтение данных из файла JSON в таблицу данных data_table
data_table = pd.read_json(file, lines=True)

# Вычисление общего количества товаров для каждой записи и добавление столбца all_quantity
data_table['all_quantity'] = data_table['products'].apply(lambda x: sum(item['quantity'] for item in x))

# Вычисление стоимости доставки tariff для каждой записи и добавление столбца tariff
data_table['tariff'] = data_table['highway_cost'] / data_table['all_quantity']

# Группировка данных по warehouse_name и вычисление среднего tariff для каждого склада
result = data_table.groupby('warehouse_name')['tariff'].mean()

# Вывод среднего tariff для каждого склада
print(result.to_string(max_rows=None))

separator = "-" * 40
print(separator)

# Преобразование таблицы данных с помощью функции explode для вычисления доходов, расходов и прибыли по каждому продукту
data_table_calculation = data_table.explode('products')
data_table_calculation['income'] = data_table_calculation.apply(
    lambda x: x['products']['price'] * x['products']['quantity'], axis=1)
data_table_calculation['expenses'] = data_table_calculation.apply(
    lambda x: x['tariff'] * x['products']['quantity'], axis=1)
data_table_calculation['profit'] = data_table_calculation['income'] + data_table_calculation['expenses']
data_table_calculation['product'] = data_table_calculation['products'].apply(lambda x: x['product'])
data_table_calculation['quantity'] = data_table_calculation['products'].apply(lambda x: x['quantity'])

# Группировка данных по product и вычисление сумм доходов, расходов, прибыли и количества для каждого продукта
product_sum = data_table_calculation.groupby('product').agg({
    'income': 'sum',
    'expenses': 'sum',
    'profit': 'sum',
    'quantity': 'sum'
}).reset_index()

# Вывод сумм доходов, расходов, прибыли и количества для каждого продукта
print(product_sum.to_string(max_rows=None))

separator = "-" * 40
print(separator)

# Вычисление прибыли для каждого заказа и создание таблицы order_profit_table с уникальными order_id и прибылью
data_table_calculation['order_profit'] = data_table_calculation['income'] + data_table_calculation['expenses']
order_profit_table = data_table_calculation[['order_id', 'order_profit']].drop_duplicates()

# Вывод таблицы с прибылью для каждого заказа
print(order_profit_table.to_string(max_rows=None))

separator = "-" * 40
print(separator)

# Вычисление средней прибыли для всех заказов и вывод ее
average_profit = order_profit_table['order_profit'].mean()
print("Средняя прибыль заказов:", average_profit)

separator = "-" * 40
print(separator)

# Продолжение обработки таблицы данных с помощью функции explode для вычисления прибыли продуктов
product_stat = data_table_calculation.explode('products')

# Группировка данных по warehouse_name и product и вычисление суммы quantity и profit для каждого продукта на каждом складе
product_stat = data_table_calculation.groupby(['warehouse_name', 'product']).agg({
    'quantity': 'sum',
    'profit': 'sum'
}).reset_index()

# Группировка данных по warehouse_name и вычисление общей прибыли для каждого склада
warehouse_profit = product_stat.groupby('warehouse_name')['profit'].sum().reset_index()
warehouse_profit.rename(columns={'profit': 'total_profit_warehouse'}, inplace=True)

# Объединение таблиц product_stat и warehouse_profit по столбцу warehouse_name
result_table = product_stat.merge(warehouse_profit, on='warehouse_name')

# Вычисление процента прибыли продукта относительно общей прибыли склада и сортировка данных по warehouse_name
result_table['percent_profit_product_of_warehouse'] = (result_table['profit'] / result_table[
    'total_profit_warehouse']) * 100

# Вывод данных о продукте, его количестве, прибыли и проценте прибыли от общей прибыли склада
print(
    result_table[['warehouse_name', 'product', 'quantity', 'profit', 'percent_profit_product_of_warehouse']].to_string(
        max_rows=None))

separator = "-" * 40
print(separator)

# Сортировка данных по проценту прибыли продукта от общей прибыли склада по убыванию
result_table.sort_values(by='percent_profit_product_of_warehouse', ascending=False, inplace=True)

# Вычисление накопленного процента прибыли продукта от общей прибыли склада
result_table['accumulated_percent_profit_product_of_warehouse'] = result_table[
    'percent_profit_product_of_warehouse'].cumsum()

# Вывод данных о продукте, его количестве, прибыли, проценте прибыли от общей прибыли склада и накопленном проценте прибыли
print(result_table.to_string(max_rows=None))

separator = "-" * 40
print(separator)

# Определение категории продукта на основе накопленного процента прибыли от общей прибыли склада
conditions = [
    (result_table['accumulated_percent_profit_product_of_warehouse'] <= 70),
    (result_table['accumulated_percent_profit_product_of_warehouse'] > 70) & (
                result_table['accumulated_percent_profit_product_of_warehouse'] <= 90)
]

categories = ['A', 'B']
result_table['category'] = np.select(conditions, categories, default='C')

# Вывод данных о продукте, его количестве, прибыли, проценте прибыли от общей прибыли склада, накопленном проценте прибыли и категории
print(result_table.to_string(max_rows=None))

separator = "-" * 40
print(separator)
