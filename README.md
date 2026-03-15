# TLG CRM - Deployment Guide

## Quick Deploy (15 minutes)

### Step 1: Create accounts (if you don't have them)
1. Go to **github.com** → Sign up (free)
2. Go to **vercel.com** → Sign up with GitHub (free)

### Step 2: Create a GitHub repository
1. Log into GitHub
2. Click the **+** button (top right) → **New repository**
3. Name it: `tlg-crm`
4. Set to **Private**
5. Click **Create repository**

### Step 3: Upload this code
1. On your new repo page, click **"uploading an existing file"** link
2. Drag and drop ALL files from this project folder:
   - `package.json`
   - `vite.config.js`
   - `index.html`
   - `src/main.jsx`
   - `src/App.jsx`
3. Click **Commit changes**

### Step 4: Deploy on Vercel
1. Go to **vercel.com/new**
2. Click **Import** next to your `tlg-crm` repo
3. Leave all settings as default
4. Click **Deploy**
5. Wait ~60 seconds — your site is live!

### Step 5: Custom domain (optional)
1. In Vercel, go to your project → **Settings** → **Domains**
2. Add `crm.lifestylegroup.com` (or whatever you want)
3. Add the DNS record Vercel gives you at your domain registrar

## Updating the app later
When Claude gives you updated code:
1. Go to your GitHub repo
2. Navigate to the file that changed (usually `src/App.jsx`)
3. Click the **pencil icon** to edit
4. Replace the content with the new code
5. Click **Commit changes**
6. Vercel auto-deploys in ~30 seconds

## Tech stack
- **Frontend:** React + Vite
- **Database:** Supabase (PostgreSQL)
- **Hosting:** Vercel (free tier)
