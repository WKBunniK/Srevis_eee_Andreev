# Схема данных и 3NF

```mermaid
erDiagram
    FLIGHTS ||--o{ SEAT_RESERVATIONS : "physical FK flight_id"
    BOOKINGS ||..|| SEAT_RESERVATIONS : "logical booking_id, separate databases"
    FLIGHTS ||..o{ BOOKINGS : "logical flight_id, separate databases"
    FLIGHTS {
        bigint id PK
        varchar flight_number "unique together with UTC departure day"
        varchar airline
        varchar origin "3 uppercase IATA letters"
        varchar destination "different from origin"
        timestamptz departure_at
        timestamptz arrival_at "after departure"
        integer total_seats "greater than zero"
        integer available_seats "0 through total_seats"
        bigint price_minor "positive RUB kopecks"
        varchar status "SCHEDULED DEPARTED CANCELLED COMPLETED"
    }
    SEAT_RESERVATIONS {
        uuid booking_id PK "logical reference to bookings.id"
        bigint flight_id FK
        integer seat_count "greater than zero"
        varchar status "ACTIVE RELEASED EXPIRED"
        timestamptz created_at
    }
    CACHE_STATE {
        integer id PK "singleton: id = 1"
        bigint version "nonnegative cache generation"
    }
    BOOKINGS {
        uuid id PK
        varchar user_id "external user identifier"
        bigint flight_id "logical reference, no physical FK"
        varchar passenger_name
        varchar passenger_email
        integer seat_count "greater than zero"
        bigint total_price_minor "positive price snapshot"
        varchar status "CONFIRMED CANCELLED"
        timestamptz created_at
    }
```

`flights`, `seat_reservations` и `cache_state` находятся в БД `flight`, `bookings` — в БД `booking`, на двух отдельных серверах PostgreSQL. Пунктирные связи — бизнес-инварианты, а не физические внешние ключи. PostgreSQL не обеспечивает FK между этими БД. При нормальном завершении создания бронь имеет ровно одну резервацию; при межсервисных сбоях возможны расхождения, описанные в [контракте](api.md).

**Ключи и ограничения.** PK — первичный ключ, уникально идентифицирует строку. `booking_id` одновременно идентификатор резервации: отдельный искусственный `reservation_id` не нужен. FK — внешний ключ, проверяет существование рейса в той же БД. Удаление рейса с резервациями запрещено стандартным поведением FK. Индекс `flight_number_day` обеспечивает уникальность `(flight_number, дата departure_at в UTC)`, даже если время вылета отличается. Все поля обязательны. Проверки CHECK ограничивают места, цены, маршрут, порядок времён и значения статусов. Поиск и список бронирований имеют отдельные индексы.

**Первая нормальная форма.** Поля атомарны: в одной ячейке нет списка пассажиров или аэропортов. Бронь имеет одного контактного пассажира и количество мест согласно заданию; персональные данные каждого пассажира не моделируются.

**Вторая нормальная форма.** Первичные ключи одноатрибутные. Каждый неключевой атрибут зависит от всего ключа. Альтернативный ключ рейса — номер и UTC-день вылета, определяющие конкретное выполнение рейса.

**Третья нормальная форма.** Неключевые атрибуты не определяют другие неключевые атрибуты. Здесь `airline` — текстовый атрибут конкретного рейса; справочника номеров, фиксирующего зависимость `flight_number → airline`, нет. Номер рейса не считается глобальным кодом авиакомпании. Аналогично `user_id` не определяет данные пассажира: один пользователь может бронировать для разных людей, а имя/email — снимок контакта конкретной брони. Если добавить справочник авиакомпаний, аэропортов или профили пользователей, их атрибуты следует вынести в отдельные таблицы.

Цена брони — исторический снимок, не копия изменяемой текущей цены рейса. В `bookings` не сохраняется дополнительно цена единицы: `seat_count` сам по себе не определяет сумму. Поле даты вылета отдельно не хранится, чтобы не дублировать функцию от `departure_at`; уникальность дня выражена индексом.

**Счётчик мест.** `available_seats` хранится по условию задачи. Это материализованный агрегат, который должен совпадать с `total_seats − SUM(seat_count активных резерваций)`. Формальные зависимости внутри таблиц соответствуют 3NF, но согласованность агрегата с другой таблицей поддерживают транзакции приложения, а не CHECK. Прямые SQL-изменения в обход сервиса способны нарушить этот инвариант.

**Версия кеша.** `cache_state` — служебная таблица из одной строки. Значение `version` меняется в той же транзакции, что рейс или резервация. `id → version`, неключевых транзитивных зависимостей нет. Общая версия позволяет проверить актуальность Redis-значения после переключения на реплику. При чтении берётся FOR SHARE, при изменении — FOR UPDATE; это сериализует мутации и исключает запись старого значения в кеш после инвалидирования.
