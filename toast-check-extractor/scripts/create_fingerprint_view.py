
import os
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5433/agm")

CREATE_VIEW_SQL = """
DROP VIEW IF EXISTS v_check_fingerprints;

CREATE OR REPLACE VIEW v_check_fingerprints AS
WITH item_categorization AS (
    SELECT 
        ci.check_id,
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Appetizer%' 
              OR m.menu_group ILIKE '%Salad%' 
              OR m.menu_group ILIKE '%Soup%'
              OR m.menu_group ILIKE '%Shellfish%' 
              OR m.menu_group ILIKE '%Charcuterie%' THEN 1 
        END) as num_starters,
        
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Entree%' 
              OR m.menu_group ILIKE '%Pasta%' 
              OR m.menu_group ILIKE '%Chicken Parm%' 
              OR m.menu_group ILIKE '%Steak%' 
              OR m.menu_group ILIKE '%Fish%'
              OR m.menu_group ILIKE '%FIRE COURSE%' THEN 1 
        END) as num_mains,
        
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Side%' 
              OR m.menu_group ILIKE '%Extra%' THEN 1 
        END) as num_sides,
        
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Dessert%' THEN 1 
        END) as num_desserts,
        
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Wine%' OR m.menu_group ILIKE '%Beer%' 
              OR m.menu_group ILIKE '%Liquor%' OR m.menu_group ILIKE '%Cocktail%'
              OR m.menu_group ILIKE '%Tequila%' OR m.menu_group ILIKE '%Vodka%'
              OR m.menu_group ILIKE '%Gin%' OR m.menu_group ILIKE '%Rum%'
              OR m.menu_group ILIKE '%Scotch%' OR m.menu_group ILIKE '%Bourbon%'
              OR m.menu_group ILIKE '%Bottle%' OR m.menu_group ILIKE '%Glass%' 
              OR m.menu_group ILIKE '%Corkage%' THEN 1 
        END) as num_alcohol,
        
        COUNT(CASE 
            WHEN m.menu_group ILIKE '%Coffee%' OR m.menu_group ILIKE '%Tea%'
              OR m.menu_group ILIKE '%Water%' OR m.menu_group ILIKE '%Soda%' THEN 1 
        END) as num_non_alc
        
    FROM check_items ci
    JOIN menu_items m ON ci.menu_item_id = m.menu_item_id
    WHERE ci.voided = FALSE
    GROUP BY ci.check_id
)
SELECT 
    c.check_id,
    c.restaurant_id,
    c.business_date,
    c.meal_period,
    c.party_size_category,
    c.guest_count,
    c.total,
    c.turnover_minutes,
    c.day_of_week,
    TRIM(TO_CHAR(c.business_date, 'Day')) as day_name,
    CASE 
        WHEN EXTRACT(MONTH FROM c.business_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM c.business_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM c.business_date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM c.business_date) IN (9, 10, 11) THEN 'Fall'
    END as season,
    COALESCE(i.num_starters, 0) as num_starters,
    COALESCE(i.num_mains, 0) as num_mains,
    COALESCE(i.num_sides, 0) as num_sides,
    COALESCE(i.num_desserts, 0) as num_desserts,
    COALESCE(i.num_alcohol, 0) as num_alcohol,
    COALESCE(i.num_non_alc, 0) as num_non_alc,
    -- Per Person Calculation (handling 0/nulls)
    CASE 
        WHEN c.guest_count > 0 THEN ROUND(c.total::numeric / c.guest_count / 100.0, 2)
        ELSE NULL 
    END as spend_per_guest_dollars
FROM checks c
LEFT JOIN item_categorization i ON c.check_id = i.check_id;
"""

def create_view():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print("Creating view v_check_fingerprints...")
                cur.execute(CREATE_VIEW_SQL)
                conn.commit()
                print("View created successfully.")
                
                # Verify
                cur.execute("SELECT COUNT(*) FROM v_check_fingerprints")
                count = cur.fetchone()[0]
                print(f"Verified: View contains {count} rows.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_view()
