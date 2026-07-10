import os

import uvicorn
from golazo.main import app


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=int(os.getenv("GOLAZO_PORT", "8765")),
        reload=False,
    )
