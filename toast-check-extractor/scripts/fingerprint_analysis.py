import os
import psycopg
import csv

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5433/agm")

def generate_fingerprints():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print("Generating Table Fingerprints...")
                
                query = """
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
                    c.party_size_category,
                    c.meal_period,
                    CASE c.day_of_week
                        WHEN 0 THEN 'Sunday'
                        WHEN 1 THEN 'Monday'
                        WHEN 2 THEN 'Tuesday'
                        WHEN 3 THEN 'Wednesday'
                        WHEN 4 THEN 'Thursday'
                        WHEN 5 THEN 'Friday'
                        WHEN 6 THEN 'Saturday'
                    END as day_name,
                    CASE 
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (12, 1, 2) THEN 'Winter'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (3, 4, 5) THEN 'Spring'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (6, 7, 8) THEN 'Summer'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (9, 10, 11) THEN 'Fall'
                    END as season,
                    COUNT(*) as sample_size,
                    ROUND(AVG(c.total)/100.0, 2) as avg_spend,
                    ROUND(SUM(c.total)::numeric / NULLIF(SUM(c.guest_count), 0) / 100.0, 2) as avg_spend_per_guest,
                    ROUND(AVG(c.turnover_minutes), 0) as avg_mins,
                    ROUND(AVG(COALESCE(i.num_starters, 0)), 1) as avg_starters,
                    ROUND(AVG(COALESCE(i.num_mains, 0)), 1) as avg_mains,
                    ROUND(AVG(COALESCE(i.num_sides, 0)), 1) as avg_sides,
                    ROUND(AVG(COALESCE(i.num_desserts, 0)), 1) as avg_desserts,
                    ROUND(AVG(COALESCE(i.num_alcohol, 0)), 1) as avg_alcohol,
                    ROUND(AVG(COALESCE(i.num_non_alc, 0)), 1) as avg_non_alc
                FROM checks c
                LEFT JOIN item_categorization i ON c.check_id = i.check_id
                WHERE c.party_size_category IS NOT NULL 
                  AND c.meal_period IS NOT NULL
                  AND c.turnover_minutes > 0 AND c.turnover_minutes < 400
                GROUP BY 
                    c.party_size_category, 
                    c.meal_period, 
                    c.day_of_week,
                    CASE 
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (12, 1, 2) THEN 'Winter'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (3, 4, 5) THEN 'Spring'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (6, 7, 8) THEN 'Summer'
                        WHEN EXTRACT(MONTH FROM c.business_date) IN (9, 10, 11) THEN 'Fall'
                    END
                HAVING COUNT(*) > 50
                ORDER BY c.party_size_category, c.meal_period, c.day_of_week
                """
                
                cur.execute(query)
                rows = cur.fetchall()
                
                # Write to CSV
                output_path = 'output/table_fingerprints.csv'
                with open(output_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    headers = [
                        'Party Size', 'Meal Period', 'Day', 'Season', 'Sample Size', 
                        'Avg Spend ($)', 'Avg Spend/Person ($)', 'Avg Time (min)', 
                        'Avg Starters', 'Avg Mains', 'Avg Sides', 'Avg Desserts', 'Avg Alcohol', 'Avg Non-Alc'
                    ]
                    writer.writerow(headers)
                    writer.writerows(rows)
                
                print(f"Detailed report saved to {output_path}")
                
                # Print Summary of Key Profiles
                print("\n--- Key Experience Profiles (Samples) ---")
                
                # Helper to print a row nicely
                def print_profile(row, label):
                    print(f"\n{label}:")
                    print(f"  Context: {row[0]} | {row[1]} | {row[2]} | {row[3]}")
                    print(f"  Metrics: Check: ${row[5]} | Per Person: ${row[6]} | Time: {row[7]} mins")
                    print(f"  Order:   {row[8]} Start | {row[9]} Main | {row[10]} Side | {row[11]} Dsst | {row[12]} Alc")

                # 1. The Classic Date Night (Couple, Dinner, Saturday, Winter)
                found = False
                for row in rows:
                    if row[0] == 'Couple' and row[1] == 'Dinner' and row[2] == 'Saturday' and row[3] == 'Winter':
                        print_profile(row, "Classic Date Night")
                        found = True
                        break
                
                # 2. The Business Lunch (Small Group, Lunch, Tuesday, Fall)
                found = False
                for row in rows:
                    if row[0] == 'Small Group' and row[1] == 'Lunch' and row[2] == 'Tuesday' and row[3] == 'Fall':
                        print_profile(row, "Business Lunch Team")
                        found = True
                        break
                        
                # 3. The Solo Diner (Solo, Dinner, Wednesday, Winter)
                found = False
                for row in rows:
                    if row[0] == 'Solo' and row[1] == 'Dinner' and row[2] == 'Wednesday' and row[3] == 'Winter':
                        print_profile(row, "Solo Weeknight Dinner")
                        found = True
                        break

                # 4. Large Party Celebration (Large Group, Dinner, Friday, Winter)
                found = False
                for row in rows:
                    if row[0] == 'Large Group' and row[1] == 'Dinner' and row[2] == 'Friday' and row[3] == 'Winter':
                        print_profile(row, "Friday Night Group Feast")
                        found = True
                        break

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_fingerprints()