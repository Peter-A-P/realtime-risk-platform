# The demo site at fraud.peterparker.ca

`site/` is the project's demo page (ADR 30): static HTML, CSS and JavaScript
against one JSON file, `site/results.json`, which `verdict site-export`
writes from the committed reports in `docs/`. There is no build step and no
backend; hosting it is a file copy and one DNS record, the same as the
portfolio's other demo pages.

## Change it

1. Edit `site/index.html`, `site/style.css` or `site/app.js`, or regenerate the
   data after a report changes:

   ```powershell
   .venv/Scripts/python.exe -m verdict.cli site-export
   ```

2. Look at it with the host's headers, content security policy included:

   ```powershell
   .venv/Scripts/python.exe -m verdict.cli site-serve
   ```

   then <http://127.0.0.1:8080>. Not `python -m http.server`: a plain file
   server sends no policy, so it shows a page the host would partly refuse.
   Open the browser console and check it is empty.

3. `tests/test_site.py` checks the data file is a fresh export, every
   interval contains its estimate, the page loads nothing off-origin and has
   no inline style, the dashboard is the first link, and the punctuation is
   plain.

## Publish it

The app is `fraud-peterparker-ca` in `rg-portfolio`, beside the other sites,
on the free plan, created with deployment source "Other" so no workflow or
credential is committed here.

```powershell
$env:SWA_CLI_DEPLOYMENT_TOKEN = az staticwebapp secrets list --name fraud-peterparker-ca --resource-group rg-portfolio --query properties.apiKey -o tsv
npx --yes @azure/static-web-apps-cli deploy ./site --env production
```

The token goes straight into one shell's environment. It is enough on its own
to publish to the site, so it never goes in the repository, the notes, or a
chat.

## The name

`fraud.peterparker.ca` is a CNAME at Cloudflare to the app's generated
hostname, **DNS only (grey cloud)**: a proxied record hides the target from
Azure's validation and the certificate is never issued. Then:

```powershell
az staticwebapp hostname set --name fraud-peterparker-ca --resource-group rg-portfolio --hostname fraud.peterparker.ca
```

Azure issues and renews the certificate itself. The dashboard keeps
`risk.peterparker.ca`, served from the instance through the Cloudflare Tunnel
(ADR 14); the two names are independent.

## Check it

- `https://fraud.peterparker.ca` serves over HTTPS with no warning.
- The top of the page says which day of the live window it is, and its first
  button opens the dashboard.
- The console is empty on the live page.
