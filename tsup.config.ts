import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    pipe: 'src/pipe/index.ts',
    'transports/websocket-adapter': 'src/transports/websocket-adapter.ts',
    'transports/webrtc-adapter': 'src/transports/webrtc-adapter.ts',
    'transports/tcp-adapter': 'src/transports/tcp-adapter.ts',
    'transports/stdio-adapter': 'src/transports/stdio-adapter.ts',
    'transports/quic-adapter': 'src/transports/quic-adapter.ts',
  },
  format: ['esm', 'cjs'],
  dts: true,
  splitting: false,
  sourcemap: true,
  clean: true,
  treeshake: true,
  outDir: 'dist',
});
