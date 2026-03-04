"""Allow running as: python -m weather_prediction"""
from weather_prediction.app import main
import asyncio

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye!")
