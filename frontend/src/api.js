export async function api(path, body, method = 'POST') {
  let response;
  try {
    response = await fetch('/api/' + path, body === undefined ? undefined : {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch {
    throw new Error('Cannot reach the API. Check that the frontend and Python API are running.');
  }
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    if (data?.detail) {
      if (Array.isArray(data.detail)) {
        throw new Error(data.detail.map(error => {
          const field = (error.loc || []).filter(part => part !== 'body').join('.');
          return [field, error.msg || 'Invalid value'].filter(Boolean).join(': ');
        }).join('; '));
      }
      throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail));
    }
    throw new Error(`API request failed (HTTP ${response.status}). Check that the Python API is running and RELAY_API_URL points to its port (default: http://127.0.0.1:8010).`);
  }
  if (data === null) {
    throw new Error('The API returned an invalid JSON response. Check RELAY_API_URL and the Python API logs.');
  }
  return data;
}
