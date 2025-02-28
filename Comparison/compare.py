import sys
import chardet

# Detect file encoding
def detect_encoding(filename):
    with open(filename, "rb") as f:
        raw_data = f.read(10000)  # Read a chunk of the file
    result = chardet.detect(raw_data)
    return result["encoding"]

# Load MapReduce output (index -> (word, timestamp/docID))
def load_mapreduce_output(filename):
    reducer_data = {}
    encoding = detect_encoding(filename)  # Detect encoding dynamically
    try:
        with open(filename, "r", encoding=encoding, errors="ignore") as file:
            for line in file:
                # Split index and (word,timestamp/docID) using tab
                parts = line.strip().split("\t")

                # Skip invalid lines
                if len(parts) < 2:
                    continue

                try:
                    # Fetch index value
                    index = int(parts[0].strip())
                except ValueError:
                    print(f"Skipping invalid index: {parts[0]}")
                    continue

                # Right split on ',' to handle cases where words contain commas
                word_timestamp = parts[1].rsplit(",", 1)

                # Skip malformed entries
                if len(word_timestamp) < 2:
                    continue

                word = word_timestamp[0].strip().replace(",", "")  # Remove commas from predicted word
                reducer_data[index] = word
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        sys.exit(1)

    return reducer_data

# Load original data and remove commas
def load_original_data(filename):
    data = {}
    encoding = detect_encoding(filename)  # Detect encoding dynamically
    try:
        with open(filename, "r", encoding=encoding, errors="ignore") as file:
            for index, word in enumerate(file.read().replace(",", "").split(), start=1):
                data[index] = word
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        sys.exit(1)

    return data

# Compare results
def compare_results(reducer_file, original_file):
    reducer_data = load_mapreduce_output(reducer_file)
    original_data = load_original_data(original_file)

    correct = 0
    total = len(reducer_data)

    for index, predicted_word in reducer_data.items():
        if index in original_data:
            actual_word = original_data[index]
            if predicted_word == actual_word:
                correct += 1
            else:
                print(f"Mismatch: {predicted_word} (Predicted) != {actual_word} (Actual) at index {index}")

    accuracy = (correct / total) * 100 if total > 0 else 0
    print(f"Comparison Accuracy: {accuracy:.2f}% ({correct}/{total} correct)")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_results.py <reducer_output_file> <original_data_file>")
        sys.exit(1)

    reducer_file = sys.argv[1]
    original_file = sys.argv[2]

    compare_results(reducer_file, original_file)