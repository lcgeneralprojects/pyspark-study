На текущий момент в репозитории находятся два решения задачи. Из них `three_table_arrangement.py`/`three_table_arrangement.ipynb` имитирует отношение между таблицами типа many-to-many с помощью трех `DataFrame`ов - `df_object` (содержит информацию об объектах, которые могут быть в коллекциях), `df_collection` (содержит информацию о коллекциях) и `df_junction` (содержит информацию о связях между объектами и коллекциями).

На текущий момент нет внедрения ограничений, включая `PRIMARY_KEY`, `NOT_NULL`, `FOREIGN_KEY`, и не происходит чтения таблиц из файлов и БД. Это решение принято с целью избежания scope creep и более ранней публикации ранней версии решения задачи. Позднее будут добавлены дальнейшие упражнения.