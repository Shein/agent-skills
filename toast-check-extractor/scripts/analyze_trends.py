import os
import psycopg
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5433/agm")

def analyze_trends():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print("--- Sales Trends ---")
                
                # Total Revenue by Date
                print("\nTotal Revenue by Date (Last 7 Days):")
                cur.execute("""
                    SELECT business_date, SUM(total)/100.0 as revenue 
                    FROM checks 
                    GROUP BY business_date 
                    ORDER BY business_date DESC 
                    LIMIT 7
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]}: ${row[1]:,.2f}")

                # Sales by Day of Week
                print("\nSales by Day of Week (0=Sunday, 6=Saturday):")
                cur.execute("""
                    SELECT day_of_week, SUM(total)/100.0 as revenue, COUNT(*) as check_count 
                    FROM checks 
                    GROUP BY day_of_week 
                    ORDER BY day_of_week
                """)
                rows = cur.fetchall()
                days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
                for row in rows:
                    if row[0] is not None and 0 <= row[0] <= 6:
                         day_name = days[row[0]]
                         print(f"  {day_name}: ${row[1]:,.2f} ({row[2]} checks)")

                # Sales by Hour
                print("\nSales by Hour:")
                cur.execute("""
                    SELECT hour_opened, SUM(total)/100.0 as revenue, COUNT(*) as check_count 
                    FROM checks 
                    WHERE hour_opened IS NOT NULL
                    GROUP BY hour_opened 
                    ORDER BY hour_opened
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]:02d}:00: ${row[1]:,.2f} ({row[2]} checks)")

                print("\n--- Menu Item Trends ---")

                # Top 5 Menu Items by Revenue
                print("\nTop 5 Menu Items by Revenue:")
                cur.execute("""
                    SELECT item_name, COALESCE(SUM(line_total), 0)/100.0 as total_revenue 
                    FROM check_items 
                    WHERE item_name IS NOT NULL
                    GROUP BY item_name 
                    ORDER BY total_revenue DESC 
                    LIMIT 5
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]}: ${row[1]:,.2f}")

                # Top 5 Menu Items by Quantity
                print("\nTop 5 Menu Items by Quantity:")
                cur.execute("""
                    SELECT item_name, COALESCE(SUM(quantity), 0) as total_qty 
                    FROM check_items 
                    WHERE item_name IS NOT NULL
                    GROUP BY item_name 
                    ORDER BY total_qty DESC 
                    LIMIT 5
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]}: {row[1]:,.0f}")
                
                print("\n--- Server Performance ---")
                
                # Top 5 Servers by Sales
                print("\nTop 5 Servers by Sales:")
                cur.execute("""
                    SELECT server_name, SUM(total)/100.0 as total_sales, AVG(tip_percentage) as avg_tip_pct
                    FROM checks 
                    WHERE server_name IS NOT NULL
                    GROUP BY server_name
                    ORDER BY total_sales DESC
                    LIMIT 5
                """)
                rows = cur.fetchall()
                for row in rows:
                    print(f"  {row[0]}: ${row[1]:,.2f} (Avg Tip: {row[2]:.1f}%)")

    except Exception as e:
        print(f"Error analyzing trends: {e}")

if __name__ == "__main__":
    analyze_trends()