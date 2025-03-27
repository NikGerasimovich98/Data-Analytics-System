# Витрина сбора ЗП для TikTok House

Витрина сбора зп для TikTok House под названием `idm_tiktok_payment` предназначена для сбора статистических метрик каждого блогера отдела “tiktok house” и расчета их заработной платы и премиальных с учетом выполненной работы. 

### Основные метрики результата витрины:
- Количество выпущенных видео по TikTok.
- Количество просмотров по каждому видео для соц. сетей TikTok и Likee.
- Количество просмотров по всем выпущенным видео для YouTube и Instagram.
- Расчет премиальных для каждого региона блогера (РБ, РФ, РК) согласно предоставленного справочника по грейдам и KPI блогеров.

**Конечный результат витрины** - получение полного отчета со всеми метриками в Excel (Google Sheets) файл.

Витрина создана согласно ТЗ: **ТЗ “Витрина сбора ЗП по TikTok House”**.

## Описание витрины данных

Сбор витрины `idm_tiktok_payment` разделён на несколько частей и объединен в одну систему для получения конечного результата.

### Части витрины `idm_tiktok_payment`:
- **Сборка `idm_tiktok_payment_short`** с помощью SQL скрипта с последующей записью результата в схему хранения историчности витрины. Приставка `short` означает сборку всех итогов по блогерам с их итоговыми премиями и просмотрами.
- **Сборка `idm_tiktok_payment_full`** с помощью SQL скрипта с последующей записью результата в схему хранения историчности витрины. Приставка `full` означает сборку всей статистики за месяц по каждому блогеру.

### Результат получается посредством следующих SQL скриптов:
- [`idm_tiktok_payment_full`](https://github.com/NikGerasimovich98/Data-Analytics-System/blob/main/sql/data%20mart/idm_tiktok_payment_full.sql)
- [`idm_tiktok_payment_short`](https://github.com/NikGerasimovich98/Data-Analytics-System/blob/main/sql/data%20mart/idm_tiktok_payment_short.sql)

## Для автоматизации сборки витрины была разработана объединенная система, которая включает в себя:
- Наполнение витрины
- Получение результата в виде Excel (Google Sheets)

### Наполнение витрины:

Витрина наполняется с помощью дага `idm_tiktok_payment_load.py`. Даг работает по следующему алгоритму:
1. **Проверка параметра `month`** на наличие в таблице `bo_tiktokers`. Эта проверка необходима, так как данные с таблиц парсятся за период 3 месяца и не хранят историчность. Без данной проверки существует вероятность удаления исторических данных без последующей их записи.
2. Получение параметра `month` — параметр месяца, за который необходимо получить результат. Параметр передается в виде имени месяца и года (`“ФЕВРАЛЬ 25/ МАРТ 25/ МАЙ 25 и т.д.”`).
3. Проверка таблиц `idm_tiktok_payment_short` и `idm_tiktok_payment_full` на наличие информации за выбранный период.
4. Если информация в этих таблицах есть, выполняется построчная очистка таблиц с помощью `DELETE`, где поле “месяц отчёта” = параметру `month`.
5. Если информация в таблицах отсутствует, переходим к пункту 4.
6. Запуск наполнения таблиц `idm_tiktok_payment_short` и `idm_tiktok_payment_full` с помощью SQL скриптов.
7. Конец работы дага.

**Код дага:** [idm_tiktok_payment_load.py](https://github.com/NikGerasimovich98/Data-Analytics-System/blob/main/dags/idm_tiktok_payment_load.py)

### Получение результата в виде Excel (Google Sheets):

Даг `idm_tiktok_payment_to_excel` предназначен для записи результатов таблиц `idm_tiktok_payment_short` и `idm_tiktok_payment_full` в Excel файл для передачи заказчику. Этот даг также использует параметр `month` для формирования отчёта за период. 
Загрузка в Excel производится с помощью pandas через получение результата таблиц (`SELECT * FROM TABLE_NAME`) в DataFrame и последующей записи в Excel с помощью `ExcelWriter`. 
Итоговым результатом является Excel файл, где:
- 1-й лист — данные из таблицы `idm_tiktok_payment_short`
- Последующие листы — данные из `idm_tiktok_payment_full`.

**Код дага:** [idm_tiktok_payment_to_excel.py](https://github.com/NikGerasimovich98/Data-Analytics-System/blob/main/dags/idm_tiktok_payment_to_excel.py)

### Объединение механизма выгрузки:

Даги `idm_tiktok_payment_load` и `idm_tiktok_payment_to_excel` объединены в один даг `idm_tiktok_payment_full` с передачей в него параметра `month`. В даге по очереди через `TriggerDagRun` запускаются даги `idm_tiktok_payment_load` и `idm_tiktok_payment_to_excel`, в которые передается параметр `month`.

## Схема витрины сбора ЗП для TikTok House
<image src="https://github.com/NikGerasimovich98/Data-Analytics-System/blob/main/docs/Airflow/Pics/%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0%20idm_tiktok_payment.drawio.png" alt="Схема витрины">
