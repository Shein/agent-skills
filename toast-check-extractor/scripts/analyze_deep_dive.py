import os
import psycopg
from decimal import Decimal

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5433/agm")

def run_deep_dive():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print("--- 1. Food vs. Beverage Mix ---")
                # Using menu_group to categorize
                cur.execute("""
                    WITH categorized_items AS (
                        SELECT 
                            ci.item_name,
                            ci.line_total,
                            m.menu_group,
                            CASE 
                                WHEN m.menu_group ILIKE '%Wine%' OR m.menu_group ILIKE '%Beer%' 
                                  OR m.menu_group ILIKE '%Liquor%' OR m.menu_group ILIKE '%Cocktail%'
                                  OR m.menu_group ILIKE '%Tequila%' OR m.menu_group ILIKE '%Vodka%'
                                  OR m.menu_group ILIKE '%Gin%' OR m.menu_group ILIKE '%Rum%'
                                  OR m.menu_group ILIKE '%Scotch%' OR m.menu_group ILIKE '%Bourbon%'
                                  OR m.menu_group ILIKE '%Bottle%' OR m.menu_group ILIKE '%Glass%'
                                  OR m.menu_group ILIKE '%Corkage%' THEN 'Alcohol'
                                WHEN m.menu_group ILIKE '%Coffee%' OR m.menu_group ILIKE '%Tea%'
                                  OR m.menu_group ILIKE '%Water%' OR m.menu_group ILIKE '%Soda%' THEN 'Non-Alc Bev'
                                ELSE 'Food'
                            END as category_type
                        FROM check_items ci
                        LEFT JOIN menu_items m ON ci.menu_item_id = m.menu_item_id
                        WHERE ci.line_total IS NOT NULL
                    )
                    SELECT 
                        category_type, 
                        SUM(line_total)/100.0 as total_sales,
                        ROUND(SUM(line_total) * 100.0 / (SELECT SUM(line_total) FROM check_items), 1) as pct_total
                    FROM categorized_items
                    GROUP BY category_type
                    ORDER BY total_sales DESC
                """)
                rows = cur.fetchall()
                for row in rows:
                    if row[0]:
                        print(f"  {row[0]}: ${row[1]:,.2f} ({row[2]}%)")

                print("\n--- 2. Top 10 'Whale' Checks ---")
                cur.execute("""
                    SELECT 
                        check_id, 
                        business_date, 
                        server_name, 
                        guest_count, 
                        total/100.0 as total_amt
                    FROM checks
                    ORDER BY total DESC
                    LIMIT 10
                """)
                rows = cur.fetchall()
                for row in rows:
                    date_str = row[1].strftime('%Y-%m-%d')
                    print(f"  Check #{row[0]} ({date_str}): ${row[4]:,.2f} (Server: {row[2]}, Guests: {row[3]})")

                print("\n--- 3. Dining Duration by Party Size ---")
                cur.execute("""
                    SELECT 
                        party_size_category, 
                        AVG(turnover_minutes) as avg_mins,
                        COUNT(*) as check_count
                    FROM checks
                    WHERE turnover_minutes IS NOT NULL AND turnover_minutes > 0 AND turnover_minutes < 400
                    GROUP BY party_size_category
                    ORDER BY avg_mins
                """)
                rows = cur.fetchall()
                for row in rows:
                    cat = row[0] if row[0] else "Unknown"
                    print(f"  {cat}: {row[1]:.0f} mins (Sample: {row[2]})")

                print("\n--- 4. Discount Analysis ---")
                # Total Discount vs Total Sales
                cur.execute("""
                    SELECT 
                        SUM(discount)/100.0 as total_discount,
                        (SELECT SUM(total)/100.0 FROM checks) as total_sales
                    FROM checks
                """)
                disc_row = cur.fetchone()
                total_disc = disc_row[0] if disc_row[0] else 0
                total_sales = disc_row[1] if disc_row[1] else 1
                disc_pct = (total_disc / total_sales) * 100
                print(f"  Total Discounts Given: ${total_disc:,.2f} ({disc_pct:.2f}% of Sales)")

                print("\n  Top 5 Discount Types:")
                cur.execute("""
                    SELECT 
                        discount_name, 
                        COUNT(*) as count, 
                        SUM(amount)/100.0 as total_amt
                    FROM check_discounts
                    GROUP BY discount_name
                    ORDER BY total_amt DESC
                    LIMIT 5
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"    {row[0]}: ${row[2]:,.2f} ({row[1]} times)")

                print("\n--- 5. Most Voided Items ---")
                cur.execute("""
                    SELECT 
                        item_name, 
                        COUNT(*) as void_count
                    FROM check_items
                    WHERE voided = TRUE
                    GROUP BY item_name
                    ORDER BY void_count DESC
                    LIMIT 10
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]}: {row[1]} voids")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_deep_dive()