import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath } from 'node:url'

// Prototype page is a second entry. Production app stays on index.html.
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      input: {
        main: fileURLToPath(new URL('./index.html', import.meta.url)),
        proto: fileURLToPath(new URL('./proto.html', import.meta.url)),
      },
    },
  },
})
