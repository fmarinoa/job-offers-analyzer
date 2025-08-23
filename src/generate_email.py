import html
from pathlib import Path
from textwrap import dedent
from typing import List, Dict

HTML_PATH = Path("data") / "email_body.html"


class EmailGenerator:
    def __init__(self, offers: List[Dict[str, str]]) -> None:
        """
        :param offers: Lista de ofertas filtradas por GeminiAnalyzer
        """
        self.offers = offers
        self.output_path = HTML_PATH

    def generate_html(self) -> None:
        html_content = dedent("""\
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body { font-family: Arial, sans-serif; }
                    .offer { margin-bottom: 20px; border-bottom: 1px solid #ccc; padding-bottom: 10px; }
                    .title { font-size: 18px; font-weight: bold; color: #333; }
                    .company, .link { color: #0073b1; text-decoration: none; }
                </style>
            </head>
            <body>
                <h2>Últimas Ofertas de Trabajo Filtradas</h2>
        """)

        if not self.offers:
            html_content += "<p>No se encontraron ofertas que coincidan con el perfil.</p>\n"
        else:
            for offer in self.offers:
                title = html.escape(offer.get("title", "Sin título"))
                employer = html.escape(offer.get("employer", "Sin empresa"))
                link_offer = html.escape(offer.get("linkOffer", "#"))
                reason = html.escape(offer.get("reason", ""))

                html_content += dedent(f"""
                    <div class="offer">
                        <p class="title">{title}</p>
                        <p class="company">{employer}</p>
                        <p><strong>Razón:</strong> {reason}</p>
                        <p><a class="link" href="{link_offer}">Ver oferta completa</a></p>
                    </div>
                """)

        html_content += "</body></html>"

        # Guardar archivo
        self.output_path.write_text(html_content, encoding="utf-8")
        print(f"✅ HTML generado en: {self.output_path}")
