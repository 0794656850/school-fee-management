M-Pesa (Daraja) STK Push Setup

Follow these steps to enable STK push in this app.

- Create a Daraja app on https://developer.safaricom.co.ke and get your:
  - Consumer Key
  - Consumer Secret
  - Lipa Na M-PESA Online Passkey
  - Business ShortCode (Paybill/Till)

- Configure the app in either of two ways:
  1) Via environment file
     - Copy `.env.example` to `.env`
     - Fill the `DARAJA_*` keys (sandbox or production). Example keys:
       - `DARAJA_ENV=sandbox`
       - `DARAJA_CONSUMER_KEY=...`
       - `DARAJA_CONSUMER_SECRET=...`
       - `DARAJA_SHORT_CODE=174379` (Sandbox examples often use 174379)
       - `DARAJA_PASSKEY=...` (your sandbox passkey)
       - `DARAJA_CALLBACK_URL=https://YOUR_PUBLIC_HOST/mpesa/callback`
     - Restart the app so Flask picks up the new env.

  2) From the Admin UI (stored in DB)
     - Login at `/admin/login` (default password: 9133)
     - Go to `/admin/mpesa`
     - Enter your Daraja credentials and save

- Callback URL
  - Must be publicly accessible (HTTPS recommended). Use a tunnel (e.g., ngrok or Cloudflare Tunnel) in dev and point it to `/mpesa/callback`.

- Using STK push
  - Open `/admin/billing`
  - If configuration is complete, the M-Pesa button is enabled.
  - Enter a Kenyan MSISDN and submit to trigger STK push.

Notes
- The app uses Sandbox or Production endpoints based on `DARAJA_ENV`.
- Credentials are read from environment variables first, then DB settings from the Admin page.
- STK push requires valid credentials; placeholders will not work.

