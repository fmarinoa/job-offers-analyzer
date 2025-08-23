from dotenv import load_dotenv

load_dotenv()

from fetch_offers import JobOffersFetcher
from analyze_offers import GeminiAnalyzer
from generate_email import EmailGenerator


def main():
    fetcher = JobOffersFetcher()
    offers, _meta = fetcher.get_all_offers(days=7)

    analyzer = GeminiAnalyzer(profile_path="src/profile.json")
    matches = analyzer.filter_offers(offers)
    # print("Matches de Gemini:\n", matches)

    generator = EmailGenerator(matches)
    generator.generate_html()


if __name__ == "__main__":
    main()
