const metadataTokenUrl = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token';

export class GcsMirror {
  constructor(bucket = process.env.GCS_MIRROR_BUCKET) {
    this.bucket = bucket;
    this.lastSync = null;
    this.lastError = null;
  }

  get enabled() { return Boolean(this.bucket); }

  status() {
    return { enabled: this.enabled, bucket: this.bucket || null, lastSync: this.lastSync, lastError: this.lastError };
  }

  async put(key, buffer, contentType = 'application/octet-stream') {
    if (!this.enabled) return { mirrored: false };
    try {
      const tokenResponse = await fetch(metadataTokenUrl, { headers: { 'Metadata-Flavor': 'Google' }, signal: AbortSignal.timeout(2500) });
      if (!tokenResponse.ok) throw new Error(`metadata token request failed (${tokenResponse.status})`);
      const { access_token: accessToken } = await tokenResponse.json();
      const url = `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(this.bucket)}/o?uploadType=media&name=${encodeURIComponent(key)}`;
      const response = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': contentType },
        body: buffer,
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error(`GCS upload failed (${response.status})`);
      this.lastSync = new Date().toISOString();
      this.lastError = null;
      return { mirrored: true };
    } catch (error) {
      this.lastError = error.message;
      return { mirrored: false, error: error.message };
    }
  }
}
