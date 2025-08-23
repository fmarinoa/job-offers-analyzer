# job-offers-analyzer

Un servicio pequeño en Python que:
1. Consume una API paginada de ofertas (`/job-offers?days=...&page=...`).
2. Pasa las ofertas a Gemini (Google Generative AI) para que filtre las que encajan con **un perfil profesional** y detecte empresas importantes.
3. Guarda el JSON de matches en `./data/matches.json`, genera `./data/email_body.html` y deja `raw_response_*.json` para debugging.
4. Diseñado para correr en GitHub Actions y enviar el HTML por correo.
