#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$1" ]; then
    echo "Usage: $0 <test_report_file>"
    exit 1
fi

REPORT_FILE="$1"

if [ ! -f "$REPORT_FILE" ]; then
    echo "Error: Report file not found: $REPORT_FILE"
    exit 1
fi

OUTPUT_DIR="${SCRIPT_DIR}/../test-reports"
mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
HTML_FILE="${OUTPUT_DIR}/test_report_${TIMESTAMP}.html"

TOTAL=$(grep -c '\[PASS\]\|\[FAIL\]' "$REPORT_FILE" || echo "0")
PASSED=$(grep -c '\[PASS\]' "$REPORT_FILE" || echo "0")
FAILED=$(grep -c '\[FAIL\]' "$REPORT_FILE" || echo "0")
WARNED=$(grep -c '\[WARN\]' "$REPORT_FILE" || echo "0")

if [ "$TOTAL" -gt 0 ]; then
    PASS_RATE=$(awk "BEGIN {printf \"%.2f\", ($PASSED/$TOTAL)*100}")
else
    PASS_RATE="0.00"
fi

cat > "$HTML_FILE" <<'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fluss CAPE Test Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .header .timestamp {
            opacity: 0.9;
            font-size: 1.1em;
        }
        
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }
        
        .stat-card {
            background: white;
            border-radius: 8px;
            padding: 25px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-card .number {
            font-size: 3em;
            font-weight: bold;
            margin: 10px 0;
        }
        
        .stat-card .label {
            color: #6c757d;
            font-size: 1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .stat-card.total .number { color: #667eea; }
        .stat-card.passed .number { color: #28a745; }
        .stat-card.failed .number { color: #dc3545; }
        .stat-card.rate .number { color: #17a2b8; font-size: 2.5em; }
        
        .progress-bar {
            height: 30px;
            background: #e9ecef;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 40px;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
            transition: width 1s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }
        
        .details {
            padding: 40px;
        }
        
        .details h2 {
            color: #333;
            margin-bottom: 20px;
            font-size: 1.8em;
        }
        
        .test-results {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
        }
        
        .test-item {
            background: white;
            margin-bottom: 10px;
            padding: 15px 20px;
            border-radius: 6px;
            border-left: 4px solid #ddd;
            display: flex;
            align-items: center;
            transition: all 0.3s ease;
        }
        
        .test-item:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        .test-item.pass {
            border-left-color: #28a745;
        }
        
        .test-item.fail {
            border-left-color: #dc3545;
        }
        
        .test-item.warn {
            border-left-color: #ffc107;
        }
        
        .test-status {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 0.85em;
            margin-right: 15px;
            min-width: 70px;
            text-align: center;
        }
        
        .test-status.pass {
            background: #d4edda;
            color: #155724;
        }
        
        .test-status.fail {
            background: #f8d7da;
            color: #721c24;
        }
        
        .test-status.warn {
            background: #fff3cd;
            color: #856404;
        }
        
        .test-name {
            font-weight: 600;
            color: #333;
            flex: 1;
        }
        
        .test-message {
            color: #6c757d;
            font-size: 0.9em;
        }
        
        .footer {
            background: #343a40;
            color: white;
            text-align: center;
            padding: 20px;
        }
        
        @media print {
            body {
                background: white;
                padding: 0;
            }
            
            .container {
                box-shadow: none;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Fluss CAPE Test Report</h1>
            <div class="timestamp">TIMESTAMP_PLACEHOLDER</div>
        </div>
        
        <div class="summary">
            <div class="stat-card total">
                <div class="label">Total Tests</div>
                <div class="number">TOTAL_PLACEHOLDER</div>
            </div>
            <div class="stat-card passed">
                <div class="label">Passed</div>
                <div class="number">PASSED_PLACEHOLDER</div>
            </div>
            <div class="stat-card failed">
                <div class="label">Failed</div>
                <div class="number">FAILED_PLACEHOLDER</div>
            </div>
            <div class="stat-card rate">
                <div class="label">Pass Rate</div>
                <div class="number">PASS_RATE_PLACEHOLDER%</div>
            </div>
        </div>
        
        <div class="progress-bar">
            <div class="progress-fill" style="width: PASS_RATE_PLACEHOLDER%">
                PASS_RATE_PLACEHOLDER% Complete
            </div>
        </div>
        
        <div class="details">
            <h2>📋 Test Results</h2>
            <div class="test-results">
EOF

while IFS= read -r line; do
    if [[ "$line" =~ ^\[([A-Z]+)\]\ (.+):\ (.+)$ ]]; then
        status="${BASH_REMATCH[1]}"
        test_name="${BASH_REMATCH[2]}"
        message="${BASH_REMATCH[3]}"
        
        status_lower=$(echo "$status" | tr '[:upper:]' '[:lower:]')
        
        cat >> "$HTML_FILE" <<EOF
                <div class="test-item $status_lower">
                    <span class="test-status $status_lower">$status</span>
                    <div>
                        <div class="test-name">$test_name</div>
                        <div class="test-message">$message</div>
                    </div>
                </div>
EOF
    fi
done < "$REPORT_FILE"

cat >> "$HTML_FILE" <<EOF
            </div>
        </div>
        
        <div class="footer">
            <p>Generated by Fluss CAPE Test Suite</p>
            <p style="margin-top: 10px; opacity: 0.8;">Apache Fluss - Multi-Protocol Compatibility Layer</p>
        </div>
    </div>
</body>
</html>
EOF

sed -i "s/TIMESTAMP_PLACEHOLDER/$(date '+%Y-%m-%d %H:%M:%S')/g" "$HTML_FILE"
sed -i "s/TOTAL_PLACEHOLDER/$TOTAL/g" "$HTML_FILE"
sed -i "s/PASSED_PLACEHOLDER/$PASSED/g" "$HTML_FILE"
sed -i "s/FAILED_PLACEHOLDER/$FAILED/g" "$HTML_FILE"
sed -i "s/PASS_RATE_PLACEHOLDER/$PASS_RATE/g" "$HTML_FILE"

echo "HTML report generated: $HTML_FILE"
echo ""
echo "Summary:"
echo "  Total: $TOTAL"
echo "  Passed: $PASSED"
echo "  Failed: $FAILED"
echo "  Pass Rate: $PASS_RATE%"
