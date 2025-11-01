# Linear Client–Server Assignment (Flask)

POST /increment
Body: {"n": <int>=0}
200: {"received": n, "result": n+1}
409 duplicate: {"error":"duplicate","n":n,"last_processed":L}
409 out-of-order -1: {"error":"out_of_order_minus_one","n":n,"last_processed":L}

Run:
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py

GET /health -> {"status":"ok"}
UML: see uml/component.png, uml/sequence.png
