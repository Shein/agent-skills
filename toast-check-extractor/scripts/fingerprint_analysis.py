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
                SELECT 
                    party_size_category,
                    meal_period,
                    day_name,
                    season,
                    COUNT(*) as sample_size,
                    ROUND(AVG(total)/100.0, 2) as avg_spend,
                    ROUND(SUM(total)::numeric / NULLIF(SUM(guest_count), 0) / 100.0, 2) as avg_spend_per_guest,
                    ROUND(AVG(turnover_minutes), 0) as avg_mins,
                    ROUND(AVG(num_starters), 1) as avg_starters,
                    ROUND(AVG(num_mains), 1) as avg_mains,
                    ROUND(AVG(num_sides), 1) as avg_sides,
                    ROUND(AVG(num_desserts), 1) as avg_desserts,
                    ROUND(AVG(num_alcohol), 1) as avg_alcohol,
                    ROUND(AVG(num_non_alc), 1) as avg_non_alc
                FROM v_check_fingerprints
                WHERE party_size_category IS NOT NULL 
                  AND meal_period IS NOT NULL
                  AND turnover_minutes > 0 AND turnover_minutes < 400
                GROUP BY 
                    party_size_category, 
                    meal_period, 
                    day_name,
                    day_of_week,
                    season
                HAVING COUNT(*) > 50
                ORDER BY party_size_category, meal_period, day_of_week
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