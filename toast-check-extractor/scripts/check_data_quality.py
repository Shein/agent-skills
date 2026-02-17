import os
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5433/agm")

def check_data():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print("--- Data Quality Check ---")
                
                # Check menu_items flags
                print("\nMenu Items Flags Sample:")
                cur.execute("""
                    SELECT item_name, is_food, is_alcohol, category, menu_group 
                    FROM menu_items 
                    LIMIT 10
                """)
                for row in cur.fetchall():
                    print(row)

                # Check discount counts
                print("\nDiscount Table Count:")
                cur.execute("SELECT COUNT(*) FROM check_discounts")
                print(cur.fetchone()[0])

                # Check discount sample
                print("\nDiscount Sample:")
                cur.execute("SELECT discount_name, amount, reason FROM check_discounts LIMIT 5")
                for row in cur.fetchall():
                    print(row)

                # Check Void sample
                print("\nVoided Items Sample:")
                cur.execute("SELECT item_name, void_reason FROM check_items WHERE voided = TRUE LIMIT 5")
                for row in cur.fetchall():
                    print(row)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_data()