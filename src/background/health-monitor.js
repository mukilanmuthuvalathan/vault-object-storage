export class HealthMonitor {
  constructor({ scan, intervalMs = 15000 }) {
    this.scan = scan;
    this.intervalMs = intervalMs;
    this.timer = null;
    this.lastHeartbeat = null;
  }

  start() {
    this.stop();
    this.timer = setInterval(async () => {
      this.lastHeartbeat = new Date().toISOString();
      await this.scan().catch(() => {});
    }, this.intervalMs);
    this.timer.unref?.();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}
