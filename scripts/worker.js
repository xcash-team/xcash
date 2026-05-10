export default {
    async fetch(request, env) {
        // 验证 Key
        if (request.headers.get('CF-Worker-Key') !== env.KEY) {
            return new Response('Unauthorized: Invalid Key', {status: 401});
        }

        // 仅支持 POST(JSON+HMAC) 与 GET(EPay 等 query string 回调)
        const method = request.method;
        if (method !== 'POST' && method !== 'GET') {
            return new Response('Only GET/POST is allowed', {status: 405});
        }

        // 取出并剔除控制头，剩余 header 原样转发
        const headers = new Headers(request.headers);
        const destination = headers.get('CF-Worker-Destination');
        headers.delete('CF-Worker-Destination');
        headers.delete('CF-Worker-Key');

        if (!destination) {
            return new Response('CF-Worker-Destination header is required', {status: 400});
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

            const response = await fetch(new Request(forwardUrl, init));

            return new Response(response.body, {
                status: response.status,
                statusText: response.statusText,
                headers: response.headers
            });

        } catch (error) {
            return new Response('Error forwarding request: ' + error.message, {status: 500});
        }
    }
};
