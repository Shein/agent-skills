#!/usr/bin/env python3
import json
from collections import defaultdict
from pathlib import Path

def verify_aggregates(json_file):
    """Verify that menu_items_summary quantities match aggregates from checks array."""
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Get menu items summary
    menu_summary = {item['Menu Item']: int(item['Item Qty']) 
                   for item in data.get('menu_items_summary', [])}
    
    # Aggregate quantities from checks array
    item_quantities = defaultdict(int)
    for check in data.get('checks', []):
        for item in check.get('data', {}).get('items', []):
            item_name = item['item_name']
            quantity = item['quantity']
            item_quantities[item_name] += quantity
    
    # Compare
    all_match = True
    mismatches = []
    
    # Check items in menu_summary
    for menu_item, summary_qty in menu_summary.items():
        actual_qty = int(item_quantities.get(menu_item, 0))
        match = summary_qty == actual_qty
        if not match:
            all_match = False
            mismatches.append({
                'item': menu_item,
                'summary_qty': summary_qty,
                'actual_qty': actual_qty,
                'difference': summary_qty - actual_qty
            })
    
    # Check for items in checks array not in summary
    for item_name, actual_qty in item_quantities.items():
        if item_name not in menu_summary:
            all_match = False
            mismatches.append({
                'item': item_name,
                'summary_qty': 0,
                'actual_qty': int(actual_qty),
                'difference': -int(actual_qty)
            })
    
    return {
        'file': str(json_file),
        'all_match': all_match,
        'summary_total': sum(menu_summary.values()),
        'actual_total': sum(int(q) for q in item_quantities.values()),
        'mismatches': mismatches,
        'mismatch_count': len(mismatches)
    }

if __name__ == '__main__':
    # Test with a single file first
    test_file = Path('/Users/shein/Desktop/agent-skills/toast-check-extractor/output/2025-01/2025-01-01.json')
    result = verify_aggregates(test_file)
    
    print(f"File: {result['file']}")
    print(f"All aggregates match: {result['all_match']}")
    print(f"Summary total quantity: {result['summary_total']}")
    print(f"Actual total quantity: {result['actual_total']}")
    print(f"Number of mismatches: {result['mismatch_count']}")
    
    if result['mismatches']:
        print("\nMismatches found:")
        for mismatch in result['mismatches']:
            print(f"  - {mismatch['item']}")
            print(f"    Summary: {mismatch['summary_qty']}, Actual: {mismatch['actual_qty']}, Diff: {mismatch['difference']}")
    else:
        print("\nNo mismatches! All aggregates are correct.")
