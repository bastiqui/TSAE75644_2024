import re
from collections import defaultdict

def parse_file(file_path):
    with open(file_path, 'r') as f:
        data = f.read()

    # Split blocks by server sections
    node_sections = re.split(r"----- \[Server@.*?\] Result:", data)
    logs = {}
    for section in node_sections[1:]:  # Ignore the first split as it's empty
        # Extract node information
        node_id_match = re.search(r"Node id: (.*?)\n", section)
        if not node_id_match:
            continue
        node_id = node_id_match.group(1)

        # Extract Recipes, Log, Summary, Ack
        recipes_match = re.search(r"Recipes: (.*?)\nLog:", section, re.DOTALL)
        log_match = re.search(r"Log: (.*?)\nSummary:", section, re.DOTALL)
        summary_match = re.search(r"Summary: (.*?)\nAck:", section, re.DOTALL)
        ack_match = re.search(r"Ack: (.*?)\n\n", section, re.DOTALL)

        logs[node_id] = {
            "Recipes": recipes_match.group(1).strip() if recipes_match else "",
            "Log": log_match.group(1).strip() if log_match else "",
            "Summary": summary_match.group(1).strip() if summary_match else "",
            "Ack": ack_match.group(1).strip() if ack_match else "",
        }

    return logs

def analyze_consistency(logs):
    discrepancies = {
        "Ack": defaultdict(list),
        "Log": defaultdict(list),
        "Recipes": defaultdict(list),
        "Summary": defaultdict(list),
    }

    for node, data in logs.items():
        for category in discrepancies.keys():
            discrepancies[category][node] = data[category]

    # Compare consistency for each category
    results = {}
    for category in discrepancies.keys():
        results[category] = compare_category(discrepancies[category])

    return results

def compare_category(category_data):
    reference = None
    mismatches = {}
    for node, values in category_data.items():
        if reference is None:
            reference = values
        elif values != reference:
            mismatches[node] = values
    return mismatches

# Run the script
file_path = 'Results_1736382695827.data'
logs = parse_file(file_path)
results = analyze_consistency(logs)

# Output results
for category, mismatches in results.items():
    print(f"\n{category} mismatches:")
    if mismatches:
        for node, data in mismatches.items():
            print(f"Node {node} has mismatched data: {data}")
    else:
        print("No mismatches found.")
