## Решение ETL для загрузки данных из PostgresQL в Elastic Search

Решение построено на поочерёдном опросе всех таблиц с поиском обновлённых записей.
В целом я выбрал этот путь для разнообразия, потому, что обычно я скорее бы предпочёл 
получить срез всех данных сразу сложным запросом. 

Скрипт `main.py` запускает генератор `extractor.pg_extract.extract()`, который питает
преобразователь-генератор `transformer.pg_to_elastic.transform()`, который питает загрузчик
данных в Elastic search: `loader.elastic_load.load()`.

Загрузчик в итоге возвращает ответы системы elastic search, которые могут быть
использованы для анализа успешности загрузки (или назначения id на стороне elastic,
в данный момент там используется тот же UUID, что и в Postgres).

Для непосредственной работы с Postgres и elastic search реализованы отдельные клиенты.

Экстрактор может работать в двух режимах - либо искать свежие небольшие изменения в каждой 
указанной таблице каждый запуск этой цепочки, либо проводить первую большую миграцию 
всех данных, используя только основную таблицу (film_work), отмечая остальные проверенными 
автоматически.

В целом слежка за кросс-таблицами не очень осмысленна, так как мы всё равно в текущем варианте
не можем инкрементно поддерживать актуальность данных (не имея возможности отслеживать
удаления, например), но мне захотелось её реализовать.

Преобразователь приводит данные к формату, идентичному схеме elastic search, группируя данные
по всем уникальным комбинациям из джойнов, которые пришли от экстрактора.

Загрузчик обертывает строки данных в bulk-формат elastic search и отправляет их 
на адрес API elastic.

Все операции поддерживают кеширование данных в хранилищах между их получением и передачей
по цепочке. Между запусками операции выгрузки данные о последних полученных записях также 
хранятся в хранилище. 

Операции экстракции и загрузки данных поддерживают повторные попытки с растущим таймаутом
при проблемах с соединением. 

Для работы с настройками применяется pydantic-settings.

### Настройка и использование

Скопируйте репозиторий на локальный компьютер с установленным Docker.

Переменные окружения для подключения к базе данных описаны в файле .env.example
Не забудьте сформировать полный .env файл в папке с файлами compose.

Для проверки работы скрипта можно воспользоваться дампом нашей настроенной БД.

Дополнительные параметры .env для проверочного подключения:
```
PG_DSN__USER=app
PG_DSN__PASSWORD=123qwe
```
Выполните в корневой папке репозитория `docker compose up -d`. В процессе запуска postgres 
данные автоматически подгрузятся из дампа базы.
Теперь начнётся операция экспорта данных, в логах будет показана выгрузка каждого фрагмента,
и затем начнётся циклический опрос таблиц БД раз в указанный интервал.

Скрипт compose для service-etl также выставляет для elastic схему movies при запуске.

После завершения первой выгрузки (сообщение "Обновление завершено"), можно работать 
с elastic search на порту 9200, например, провести тесты Postman.

---
# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.
