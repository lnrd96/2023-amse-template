source ../../venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m unittest unit_tests/test_DatabaseHandler.py
python3 -m unittest unit_tests/test_Pipeline.py