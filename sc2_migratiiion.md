 

###   
**SCD Type 2 при миграции** — это не просто «добавить колонки», а **процесс инкрементальной загрузки с историзацией изменений** из OLTP-источника (например, PostgreSQL) в аналитическое хранилище (например, Parquet в S3 или таблицу в Greenplum).

---


#### 1. **Источник (PostgreSQL)**  
Допустим, у тебя есть таблица `clients`:
```sql
id | name  | segment    | updated_at
1  | Иван  | Standard   | 2026-01-10
```
→ Но **в OLTP нет истории**: при смене сегмента `segment` просто перезаписывается, а `updated_at` обновляется.

#### 2. **Цель (аналитика)**  
Таблица `dim_client_scd2`:
```text
client_id | name | segment | valid_from | valid_to   | is_current
1         | Иван | Standard| 2025-01-01 | 2026-01-26 | false
1         | Иван | Premium | 2026-01-27 | 9999-12-31 | true
```

#### 3. **Как мигрировать с поддержкой SCD2?**

##### Шаг A: **Выявить изменения**
- Запомни последнюю дату миграции (например, в Airflow: `{{ ds }}` или в отдельной таблице `etl_log`).
- Выбери из источника строки, где `updated_at > last_migration_time`.

##### Шаг B: **Сравнить с текущей версией в DWH**
- Для каждого изменённого клиента:
  - Если `segment` **не изменился** → пропустить.
  - Если **изменился** → триггерить SCD2-логику.

> Это можно сделать в Python (pandas) или SQL (JOIN между источником и DWH).

##### Шаг C: **Обновить DWH**
- Закрыть старую запись (`valid_to = вчера`, `is_current = false`)
- Вставить новую (`valid_from = сегодня`, `is_current = true`)

##### Шаг D: **Записать в Parquet / Greenplum**
- В Parquet: партиционируй по `valid_from` или `client_id`.
- В Greenplum: используй distribution key по `client_id`.

---

###  
> «При миграции клиентских данных из PostgreSQL в Parquet я реализовывал логику, близкую к SCD Type 2. Каждый день выгружались только изменённые строки (по `updated_at`). В Python я сравнивал новое значение с последней версией в хранилище. Если сегмент менялся — создавалась новая запись с датой начала действия, а предыдущая помечалась как неактуальная. Это позволяло строить исторические отчёты без перезаписи данных».

 
---
 псевдокод (или SQL + Python) для одного цикла такой миграции. 
---
Отлично, Илья. Вот **мини-задача в формате, близком к твоей реальной практике**:  
миграция клиентов из PostgreSQL в Parquet с поддержкой **SCD Type 2** — всё, что можно запустить локально, без продакшена.

---

###  Цель цикла:
Обработать изменения за **один день** (например, `2026-01-27`) и обновить аналитическую таблицу (`dim_client_scd2`), сохраняя историю.

---

###

**Источник (PostgreSQL):**  
Таблица `clients` (OLTP):
```sql
id | name  | segment    | updated_at
1  | Иван  | Premium    | 2026-01-27 10:00:00  -- изменился сегодня
2  | Мария | Standard   | 2026-01-25           -- не менялась
```

**Аналитическое хранилище (Parquet / или временная таблица в памяти):**  
Файл `dim_client_scd2.parquet` (последнее состояние на 2026-01-26):
```text
client_id | name  | segment  | valid_from | valid_to   | is_current
1         | Иван  | Standard | 2025-01-01 | 9999-12-31 | true
```

---

### Псевдокод + Python/SQL (реалистичный, как в Airflow)

```python
# 1. Получить дату обработки (например, из Airflow: {{ ds }})
run_date = "2026-01-27"

# 2. Загрузить изменения из источника (PostgreSQL)
changed_clients = pg_query(f"""
    SELECT id, name, segment, updated_at
    FROM clients
    WHERE DATE(updated_at) = '{run_date}'
""")

# 3. Загрузить текущую версию dim_client_scd2 (из Parquet)
current_dim = read_parquet("s3://my-bucket/dim_client_scd2.parquet")
# Оставить только is_current = True
active_dim = current_dim[current_dim["is_current"] == True]

# 4. Найти, у кого изменился сегмент
to_update = []
for row in changed_clients:
    client_id = row["id"]
    new_segment = row["segment"]
    
    # Найти текущую запись в DWH
    current_record = active_dim[active_dim["client_id"] == client_id]
    
    if not current_record.empty:
        old_segment = current_record.iloc[0]["segment"]
        if old_segment != new_segment:
            # Требуется SCD2-обновление
            to_update.append({
                "client_id": client_id,
                "old_segment": old_segment,
                "new_segment": new_segment,
                "name": row["name"]
            })

# 5. Обновить историю: закрыть старые, добавить новые
updates = []
for item in to_update:
    # Закрыть старую запись
    updates.append({
        "client_id": item["client_id"],
        "name": item["name"],
        "segment": item["old_segment"],
        "valid_from": current_record["valid_from"].iloc[0],
        "valid_to": day_before(run_date),  # '2026-01-26'
        "is_current": False
    })
    # Добавить новую запись
    updates.append({
        "client_id": item["client_id"],
        "name": item["name"],
        "segment": item["new_segment"],
        "valid_from": run_date,           # '2026-01-27'
        "valid_to": "9999-12-31",
        "is_current": True
    })

# 6. Объединить: удалить старые активные записи, добавить обновлённые
new_dim = current_dim.copy()
# Удаляем старые активные версии тех, кого обновляем
ids_to_replace = [u["client_id"] for u in to_update]
new_dim = new_dim[~(
    (new_dim["client_id"].isin(ids_to_replace)) & 
    (new_dim["is_current"] == True)
)]
# Добавляем новые записи
new_dim = pd.concat([new_dim, pd.DataFrame(updates)], ignore_index=True)

# 7. Сохранить обратно в Parquet
write_parquet(new_dim, "s3://my-bucket/dim_client_scd2.parquet")
```

---



 
