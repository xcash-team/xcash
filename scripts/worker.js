// 转发到商户 webhook 时只放行下列 header，避免 Cloudflare 自动注入的
// CF-Connecting-IP / X-Forwarded-For / True-Client-IP 等头携带 xcash 出网 IP，
// 击穿藏 IP 设计。代理鉴权头 CF-Worker-Key/Destination 不在白名单内自然被剔除。
const FORWARD_HEADER_ALLOWLIST = [
    'content-type',
    'xc-appid',
    'xc-nonce',
    'xc-timestamp',
    'xc-signature',
];

// 常量时间字符串比较，避免基于响应时长的 key 猜测
function timingSafeEqual(a, b) {
    if (a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) {
        diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    }
    return diff === 0;
}

function isUnsafeIPv4(hostname) {
    const parts = hostname.split('.');
    if (parts.length !== 4) return false;
    const nums = parts.map((part) => Number(part));
    if (nums.some((n, i) => !Number.isInteger(n) || n < 0 || n > 255 || String(n) !== parts[i])) {
        return false;
    }
    const [a, b] = nums;
    return (
        a === 10 ||
        a === 127 ||
        a === 0 ||
        a >= 224 ||
        (a === 100 && b >= 64 && b <= 127) ||
        (a === 169 && b === 254) ||
        (a === 172 && b >= 16 && b <= 31) ||
        (a === 192 && b === 0) ||
        (a === 192 && b === 168) ||
        (a === 198 && (b === 18 || b === 19 || b === 51)) ||
        (a === 203 && b === 0)
    );
}

function isSafeDestination(destination) {
    let url;
    try {
        url = new URL(destination);
    } catch (_error) {
        return false;
    }

    if (url.protocol !== 'https:') return false;
    const hostname = url.hostname.toLowerCase().replace(/\.$/, '');
    if (!hostname || hostname === 'localhost' || hostname.endsWith('.localhost')) {
        return false;
    }
    if (isUnsafeIPv4(hostname)) return false;
    if (hostname.includes(':')) {
        return false;
    }
    return true;
}

export default {
    async fetch(request, env) {
        // 验证 Key
        const provided = request.headers.get('CF-Worker-Key') || '';
        if (!timingSafeEqual(provided, env.KEY || '')) {
            return new Response('Unauthorized: Invalid Key', {status: 401});
        }

        // 仅支持 POST(JSON+HMAC) 与 GET(EPay 等 query string 回调)
        const method = request.method;
        if (method !== 'POST' && method !== 'GET') {
            return new Response('Only GET/POST is allowed', {status: 405});
        }

        const destination = request.headers.get('CF-Worker-Destination');
        if (!destination) {
            return new Response('CF-Worker-Destination header is required', {status: 400});
        }
        if (!isSafeDestination(destination)) {
            return new Response('Unsafe destination', {status: 400});
        }

        // 从空 Headers 出发显式构造，只带白名单内的头
        const headers = new Headers();
        for (const name of FORWARD_HEADER_ALLOWLIST) {
            const v = request.headers.get(name);
            if (v !== null) headers.set(name, v);
        }

        try {
            let forwardUrl = destination;

            if (method === 'GET') {
                // 把进入 worker 的 query string 合并到目标 URL，保留商户 notify_url
                // 自带的参数；GET 回调（如 EPay）签名信息就在 query 里，需逐字透传。
                const targetUrl = new URL(destination);
                const incoming = new URL(request.url).searchParams;
                for (const [k, v] of incoming) {
                    targetUrl.searchParams.append(k, v);
                }
                forwardUrl = targetUrl.toString();
            }

            const init = {method, headers};
            if (method === 'POST') {
                init.body = await request.arrayBuffer();
            }

            const response = await fetch(new Request(forwardUrl, {...init, redirect: 'manual'}));

            return new Response(response.body, {
                status: response.status,
                statusText: response.statusText,
                headers: response.headers
            });

        } catch (error) {
            return new Response('Error forwarding request', {status: 500});
        }
    }
};
