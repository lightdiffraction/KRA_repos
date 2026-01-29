структура:\
Навроцкая_ОАИТ.pdf - описание базы\
save_denormalized_to_csv.sql - скрипт для сохранения денормализованных данных в csv из page\
demormalized_pages.csv - денормализованные данные\
complete_etl.py - ETL для загрузки денормализованных данных в базу\
run_my_etl.py - запустить etl\
repositories - CRUD-обвязки на таблицы page и category\
page_service.py, category_service.py - FastAPI веб сервисы для взаимодействия с таблицами page и category через REST\
aggregate.py - сервис обращающийся к двум описанным выше\
UI.html - открыть в браузере, интерфейс к сервису аггрегатору\
test_app.py, pytest.ini - тестирование аггрегатора
