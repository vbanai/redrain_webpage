
console.log("[SessionManager] loaded");
let idleTimeout = 60 * 60 * 1000;
let lastActivity = Date.now();

function resetActivity() {
    lastActivity = Date.now();
}

document.addEventListener('mousemove', resetActivity);
document.addEventListener('keydown', resetActivity);
document.addEventListener('scroll', resetActivity);

async function sendHeartbeat() {
    if (document.hidden) return;  

    const now = Date.now();

    if (now - lastActivity < idleTimeout) {
        try {
            const resp = await fetch('/heartbeat', {
                method: 'POST',
                credentials: 'include'
            });

            if (resp.status === 401) {
                window.location.href = "/";
                return;
            }

            console.log("[Heartbeat] sent", resp.status);
        } catch (e) {
            console.error("[Heartbeat] failed", e);
        }
    }
}

// run every minute
setInterval(sendHeartbeat, 60000);

// when user get back to the page after more then 1 hour idle, we check heartbeat and it will throw 400 if we are out of 60 minutes and we will automatically be redirected to "/"

// run when user returns to tab
async function checkSession() {
    try {
        const resp = await fetch('/heartbeat', {
            method: 'POST',
            credentials: 'include'
        });

        if (resp.status === 401) {
            window.location.href = "/";
        }
    } catch (e) {
        console.error("Session check failed", e);
    }
}

checkSession();

// Fires when user clicks back into the tab
window.addEventListener("focus", checkSession);

// Fires when tab becomes visible (more reliable)
document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
        checkSession();
    }
});