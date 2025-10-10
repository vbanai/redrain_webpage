

// Inactivity Timeout Variables
let inactivityTime = 8 * 60 * 60 * 1000; // 5 minutes in milliseconds
let inactivityTimer;

// Function to log out or redirect after inactivity
function handleInactivity() {
  // Redirect to the logout route with a query parameter indicating inactivity
  window.location.href = '/logout?inactivity=true';
}

// Function to reset the inactivity timer
function resetInactivityTimer() {
//   console.log('resetInactivityTimer called');
  clearTimeout(inactivityTimer);
  inactivityTimer = setTimeout(handleInactivity, inactivityTime);
}

// Event listeners with added debug logs
window.onload = () => {
    console.log('Page loaded');
    resetInactivityTimer();
};
window.onmousemove = () => {
    // console.log('Mouse moved');
    resetInactivityTimer();
};
window.onkeypress = () => {
    console.log('Key pressed');
    resetInactivityTimer();
};
window.onclick = () => {
    console.log('Click detected');
    resetInactivityTimer();
};
window.onscroll = () => {
    console.log('Scroll detected');
    resetInactivityTimer();
};