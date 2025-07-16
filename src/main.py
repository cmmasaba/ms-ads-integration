"""Driver code"""
from app.main import BingAds

def main() -> None:
    """Entrypoint"""
    app = BingAds()
    logger = app.logger

    logger.info('Starting automation.')
    app.start()
    logger.info('Finished successfully.')

if __name__ == '__main__':
    main()
