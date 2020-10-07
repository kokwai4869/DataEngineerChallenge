from pathlib import Path
import Analyzer

FILENAME = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
weblog_file = Path(FILENAME)
if weblog_file.exists():
    print("Loaded file")
    print("Processing to analyze the loaded dataset.....")
    # Initialize the class
    paypay = Analyzer.PaypayAnalyzer(FILENAME)
    paypay.show_goal_tasks()
    print("Process completed")
else:
    print(f"{FILENAME} does not exist.")