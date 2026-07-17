import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 19110,
    strictPort: true,
    allowedHosts: ['dedupe16.dev.portnumber53.com', 'dedupe180.dev.portnumber53.com'],
    proxy: {
      '/api': 'http://127.0.0.1:19111',
    },
  },
})
