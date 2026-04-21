CREATE PROCEDURE porkdata.CreateAndPopulateTables
AS
BEGIN
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porkdata'
        AND TABLE_NAME = 'fact_trade'
    )
    BEGIN
        create table GoldWarehouse.porkdata.fact_trade
        (
            partner varchar(255),
            flow varchar(6),
            product_group varchar(255),
            product_weight_in_tonnes float,
            carcase_weight_in_tonnes float,
            value_in_thousand_euro float,
            trade_key varchar(255),
            last_month int
        )
    END

    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porkdata'
        AND TABLE_NAME = 'fact_prices'
    )
    BEGIN
        create table GoldWarehouse.porkdata.fact_prices
        (
            pig_class varchar(30),
            price float,
            unit varchar(15),
            week_number int,
            prices_key varchar(255)
        )
    END

    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porkdata'
        AND TABLE_NAME = 'fact_production'
    )
    BEGIN
        create table GoldWarehouse.porkdata.fact_production
        (
            heads float,
            kg_per_head float,
            tonnes float,
            prod_key varchar(255),
            country_group varchar(30)
        )
    END

    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porkdata'
        AND TABLE_NAME = 'bridge_member_date'
    )
    BEGIN
        create table GoldWarehouse.porkdata.bridge_member_date
        (
            bridge_key varchar(255),
            member_state varchar(30),
            year_month varchar(10)
        )
    END

    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porkdata'
        AND TABLE_NAME = 'dim_date'
    )
    BEGIN
        create table GoldWarehouse.porkdata.dim_date
        (
            year_month varchar(10),
            year int,
            month_number int,
            month_name varchar(15),
            month_short_name varchar(3),
            year_month_int int
        )
    END

    UPDATE t1
    SET
        t1.product_weight_in_tonnes = s1.product_weight_in_tonnes,
        t1.carcase_weight_in_tonnes = s1.carcase_weight_in_tonnes,
        t1.value_in_thousand_euro = s1.value_in_thousand_euro,
        t1.last_month = s1.last_month
    FROM porkdata.fact_trade t1
    JOIN (
        SELECT
            s_trade.partner AS partner,
            s_trade.flow AS flow,
            s_trade.product_group AS product_group,
            SUM(s_trade.product_weight_in_tonnes) AS product_weight_in_tonnes,
            SUM(s_trade.carcase_weight_in_tonnes) AS carcase_weight_in_tonnes,
            SUM(s_trade.value_in_thousand_euro) AS value_in_thousand_euro,
            CONCAT(s_trade.member_state, '-', FORMAT(DATEFROMPARTS(s_trade.marketing_year, s_trade.month_order, 1), 'yyyy-MM')) as trade_key,
            m.max_last_month AS last_month
        FROM [SilverLakehouse].[dbo].[trade_silver] s_trade
        JOIN (
            SELECT
                partner,
                marketing_year,
                MAX(month_order) AS max_last_month
            FROM [SilverLakehouse].[dbo].[trade_silver]
            WHERE marketing_year = (SELECT MAX(marketing_year) FROM [SilverLakehouse].[dbo].[trade_silver])
            GROUP BY partner, marketing_year
        ) m
        ON s_trade.partner = m.partner
        GROUP BY s_trade.member_state, s_trade.partner, s_trade.flow, s_trade.marketing_year, s_trade.month_order, s_trade.product_group, m.max_last_month
    ) s1
    ON t1.trade_key = s1.trade_key
    AND t1.partner = s1.partner
    AND t1.flow = s1.flow
    AND t1.product_group = s1.product_group;

    INSERT INTO porkdata.fact_trade (
        partner, flow, product_group, product_weight_in_tonnes,
        carcase_weight_in_tonnes, value_in_thousand_euro, trade_key, last_month
    )
    SELECT
        s1.partner, s1.flow, s1.product_group, s1.product_weight_in_tonnes,
        s1.carcase_weight_in_tonnes, s1.value_in_thousand_euro, s1.trade_key, s1.last_month
    FROM (
        SELECT
            s_trade.partner AS partner,
            s_trade.flow AS flow,
            s_trade.product_group AS product_group,
            SUM(s_trade.product_weight_in_tonnes) AS product_weight_in_tonnes,
            SUM(s_trade.carcase_weight_in_tonnes) AS carcase_weight_in_tonnes,
            SUM(s_trade.value_in_thousand_euro) AS value_in_thousand_euro,
            CONCAT(s_trade.member_state, '-', FORMAT(DATEFROMPARTS(s_trade.marketing_year, s_trade.month_order, 1), 'yyyy-MM')) as trade_key,
            m.max_last_month AS last_month
        FROM [SilverLakehouse].[dbo].[trade_silver] s_trade
        JOIN (
            SELECT
                partner,
                marketing_year,
                MAX(month_order) AS max_last_month
            FROM [SilverLakehouse].[dbo].[trade_silver]
            WHERE marketing_year = (SELECT MAX(marketing_year) FROM [SilverLakehouse].[dbo].[trade_silver])
            GROUP BY partner, marketing_year
        ) m
        ON s_trade.partner = m.partner
        GROUP BY s_trade.member_state, s_trade.partner, s_trade.flow, s_trade.marketing_year, s_trade.month_order, s_trade.product_group, m.max_last_month
    ) s1
    WHERE NOT EXISTS (
        SELECT 1
        FROM porkdata.fact_trade t1
        WHERE t1.trade_key = s1.trade_key
        AND t1.partner = s1.partner
        AND t1.flow = s1.flow
        AND t1.product_group = s1.product_group
    )

    UPDATE t2
    SET
        t2.price = s2.price,
        t2.unit = s2.unit
    FROM porkdata.fact_prices t2
    JOIN (
        SELECT
            s_prices.pig_class as pig_class,
            s_prices.price as price,
            s_prices.unit as unit,
            s_prices.week_number as week_number,
            CONCAT(s_prices.member_state_name, '-', FORMAT(DATEFROMPARTS(s_prices.year, s_prices.month_number, 1), 'yyyy-MM')) as prices_key
        FROM [SilverLakehouse].[dbo].[prices_silver] s_prices
        WHERE pig_class = 'Average S + E'
    ) s2
    ON t2.prices_key = s2.prices_key
    AND t2.pig_class = s2.pig_class
    AND t2.week_number = s2.week_number

    INSERT INTO porkdata.fact_prices (
        prices_key,
        pig_class,
        week_number,
        price,
        unit
    )
    SELECT
        s2.prices_key,
        s2.pig_class,
        s2.week_number,
        s2.price,
        s2.unit
    FROM (
        SELECT
            s_prices.pig_class AS pig_class,
            s_prices.price AS price,
            s_prices.unit AS unit,
            s_prices.week_number AS week_number,
            CONCAT(s_prices.member_state_name, '-', FORMAT(DATEFROMPARTS(s_prices.year, s_prices.month_number, 1), 'yyyy-MM')) AS prices_key
        FROM [SilverLakehouse].[dbo].[prices_silver] s_prices
        WHERE s_prices.pig_class = 'Average S + E'
    ) s2
    WHERE NOT EXISTS (
        SELECT 1
        FROM porkdata.fact_prices t2
        WHERE t2.prices_key = s2.prices_key
        AND t2.pig_class = s2.pig_class
        AND t2.week_number = s2.week_number
    )


    UPDATE t3
    SET
        t3.heads = s3.heads,
        t3.kg_per_head = s3.kg_per_head,
        t3.tonnes = s3.tonnes,
        t3.country_group = s3.country_group
    FROM porkdata.fact_production t3
    JOIN (
        SELECT s_prod.heads as heads,
            s_prod.kg_per_head as kg_per_head,
            s_prod.tonnes * 1000 as tonnes,
            CONCAT(s_prod.member_state_name, '-', FORMAT(CONVERT(DATE, CONCAT(s_prod.month, ' ', 1, ' ', s_prod.year)), 'yyyy-MM')) as prod_key,
            CASE 
                WHEN r.rnk <= 7 THEN s_prod.member_state_name
                ELSE 'Other'
            END AS country_group
        FROM [SilverLakehouse].[dbo].[production_silver] s_prod
        JOIN (
            SELECT
                member_state_name,
                RANK() OVER (ORDER BY SUM(tonnes) DESC) AS rnk
            FROM [SilverLakehouse].[dbo].[production_silver]
            WHERE year >= YEAR(GETDATE()) - 4 
            GROUP BY member_state_name) r
        ON s_prod.member_state_name = r.member_state_name
    ) s3
    ON t3.prod_key = s3.prod_key

    INSERT INTO porkdata.fact_production (
        prod_key,
        heads,
        kg_per_head,
        tonnes,
        country_group
    )
    SELECT
        s3.prod_key,
        s3.heads,
        s3.kg_per_head,
        s3.tonnes,
        s3.country_group
    FROM (
        SELECT
            s_prod.heads AS heads,
            s_prod.kg_per_head AS kg_per_head,
            s_prod.tonnes * 1000 AS tonnes,
            CONCAT(s_prod.member_state_name, '-', FORMAT(CONVERT(DATE, CONCAT(s_prod.month, ' ', 1, ' ', s_prod.year)), 'yyyy-MM')) AS prod_key,
            CASE 
                WHEN r.rnk <= 7 THEN s_prod.member_state_name
                ELSE 'Other'
            END AS country_group
        FROM [SilverLakehouse].[dbo].[production_silver] s_prod
        JOIN (
            SELECT
                member_state_name,
                RANK() OVER (ORDER BY SUM(tonnes) DESC) AS rnk
            FROM [SilverLakehouse].[dbo].[production_silver]
            WHERE year >= YEAR(GETDATE()) - 4 
            GROUP BY member_state_name
        ) r
        ON s_prod.member_state_name = r.member_state_name
    ) s3
    WHERE NOT EXISTS (
        SELECT 1
        FROM porkdata.fact_production t3
        WHERE t3.prod_key = s3.prod_key
    );

    UPDATE t4
    SET
        t4.member_state = s4.member_state,
        t4.year_month = s4.year_month
    FROM porkdata.bridge_member_date t4
    JOIN (
        SELECT
            trade_key AS bridge_key,
            LEFT(trade_key, CHARINDEX('-', trade_key) - 1) AS member_state,
            RIGHT(trade_key, 7) AS year_month
        FROM porkdata.fact_trade

        UNION

        SELECT
            prices_key AS bridge_key,
            LEFT(prices_key, CHARINDEX('-', prices_key) - 1) AS member_state,
            RIGHT(prices_key, 7) AS year_month
        FROM porkdata.fact_prices

        UNION

        SELECT
            prod_key AS bridge_key,
            LEFT(prod_key, CHARINDEX('-', prod_key) - 1) AS member_state,
            RIGHT(prod_key, 7) AS year_month
        FROM porkdata.fact_production
    ) s4
    ON t4.bridge_key = s4.bridge_key;

    -- 🆕 Wstawienie brakujących rekordów
    INSERT INTO porkdata.bridge_member_date (
        bridge_key,
        member_state,
        year_month
    )
    SELECT
        s4.bridge_key,
        s4.member_state,
        s4.year_month
    FROM (
        SELECT
            trade_key AS bridge_key,
            LEFT(trade_key, CHARINDEX('-', trade_key) - 1) AS member_state,
            RIGHT(trade_key, 7) AS year_month
        FROM porkdata.fact_trade

        UNION

        SELECT
            prices_key AS bridge_key,
            LEFT(prices_key, CHARINDEX('-', prices_key) - 1) AS member_state,
            RIGHT(prices_key, 7) AS year_month
        FROM porkdata.fact_prices

        UNION

        SELECT
            prod_key AS bridge_key,
            LEFT(prod_key, CHARINDEX('-', prod_key) - 1) AS member_state,
            RIGHT(prod_key, 7) AS year_month
        FROM porkdata.fact_production
    ) s4
    WHERE NOT EXISTS (
        SELECT 1
        FROM porkdata.bridge_member_date t4
        WHERE t4.bridge_key = s4.bridge_key
    )

    INSERT INTO porkdata.dim_date (
        year_month,
        year,
        month_number,
        month_name,
        month_short_name,
        year_month_int
    )
    SELECT
        DISTINCT bmd.year_month,
        YEAR(CAST(bmd.year_month + '-01' AS DATE)) AS year,
        MONTH(CAST(bmd.year_month + '-01' AS DATE)) AS month_number,
        DATENAME(MONTH, CAST(bmd.year_month + '-01' AS DATE)) AS month_name,
        LEFT(DATENAME(MONTH, CAST(bmd.year_month + '-01' AS DATE)), 3) AS month_short_name,
        CAST(REPLACE(bmd.year_month, '-', '') AS INT) AS year_month_int
    FROM porkdata.bridge_member_date bmd
    WHERE NOT EXISTS (
        SELECT 1
        FROM porkdata.dim_date dd
        WHERE dd.year_month = bmd.year_month
    )

END