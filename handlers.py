# handlers.py
async def handle_score(payload: dict):
    # plug in your Lyra scorer here
    return {"score": 0.42}

HANDLERS = {
    "score_request": handle_score,
}