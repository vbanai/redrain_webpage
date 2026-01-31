             // Storing the user id to track if this user is recurring





// ----------------------------------------------
// First act: we have the loader file on azure blob, and when host page is loading it 
// gets the loader as its own js. It will run it, loader check userid is stored if not create one
// and send its Post request to the backend route "/" where in the url request it includes client id, user id apikey 
// if user id is included it is fine backend doesn't create newone, if post validated the loader
// goes into the get request process send another request wiwth the  url again, same on backend first
// check the userid if not create one, and finally serve the html pass the variables and run the popup js as well
// and we reach the socket creation part where we send the auth params to the Socket.IO backend. 
//Is it a Socket.IO/WebSocket request? → handled by `sio`. Otherwise → passed to `fastapi_app` (HTTP routes) 



      // CREATING and ADMINISTRATE A WEBSOCKET CONNECTION TO THE BACKEND





let botMode=client_initial_mode
let user_id_forMode=userId
let client_id_forMode=client_code
let overallMode=client_initial_mode



// Send user message to the server via Socket.IO
// const urlParams = new URLSearchParams(window.location.search);
// const client_code = urlParams.get('client_code');
// const api_key = urlParams.get('api_key');


document.addEventListener("DOMContentLoaded", function() {

            // Get URL parameters  
        
  // Loader sets the iframe URL after POST validation: iframe.src = `${backend_url}/?client_code=${encodeURIComponent(client_code)}&iframeWidth=${iframeWidth}&chatBodyHeight=${chatBodyHeight}&user_id=${encodeURIComponent(userId)}`;
  // This triggers a GET request from the browser to the backend, because the iframe is now pointing to that URL.
  // Backend GET route
  // Backend receives the GET request.
  // Reads query parameters from URL (client_code, user_id, iframeWidth, chatBodyHeight).
  // Generates the popup HTML (softcodedPOPUP.html) and injects template variables, including user_id.
  // Sends this HTML back to the iframe.
  // The browser loads the iframe with that URL: http://backend-url/?client_code=abc123&iframeWidth=420&chatBodyHeight=300&user_id=xyz
  // Inside the iframe, popup.js runs, and window.location.search contains: "?client_code=abc123&iframeWidth=420&chatBodyHeight=300&user_id=xyz"


const urlParams = new URLSearchParams(window.location.search);
let width = urlParams.get('iframeWidth') || 420; // Default to 420px

// Select elements
const chatbotPopup = document.querySelector('.chatbot-popup');
const chatBody = document.querySelector('.chat-body');



// Listen for size updates from the parent window  //közvetlenül a browsertől(iframet tartalmazó) kapjuk az adatokat
  // Listen for size updates from the parent window  //közvetlenül a browsertől(iframet tartalmazó) kapjuk az adatokat
  // As I have in loader:   if (iframe && iframe.contentWindow) { this references the window object of the iframe.
                  // iframe.contentWindow.postMessage( this sends a message from the parent to that iframe.
                  //     { iframeWidth, chatBodyHeight }, The object { iframeWidth, chatBodyHeight } is the payload.
                  //     "*"

});
//requestAnimationFrame make sure smooth transition when parameters changes



const chatBody=document.querySelector(".chat-body")
const messageInput=document.querySelector(".message-input")
const sendMessageButton=document.querySelector("#send-message")
const fileInput=document.querySelector("#file-input")
const pastePreview = document.querySelector(".paste-preview");
const fileUploadWrapper=document.querySelector(".file-upload-wrapper");
const fileCancelButton=document.querySelector("#file-cancel");

const closeChatbot=document.querySelector("#close-chatbot");



function showTempMessage(text) {
  const footer = document.querySelector(".chat-footer");
  if (!footer) return;

  // Remove any existing temp message first
  const existing = footer.querySelector(".temp-message");
  if (existing) existing.remove();

  // Create new message
  const tempMsg = document.createElement("div");
  tempMsg.className = "temp-message";
  tempMsg.textContent = text;

  footer.appendChild(tempMsg);

  // Auto-remove after 4 seconds
  setTimeout(() => tempMsg.remove(), 4000);
}






const userData={
message:null,
file:{
data:null,
mime_type: null
}
}


const initalInputHeight=messageInput.scrollHeight;


// Create message element with dynamic classes and return it
const createMessageElement = (content, ...classes) => {
const div=document.createElement("div");
div.classList.add("message", ...classes);
div.innerHTML=content;
return div;
}



const handleOutgoingMessage=(e)=>{

e.preventDefault();
userData.message=messageInput.value.trim()


if (!userData.message) return;
// Send the message to the backend



messageInput.value="";
fileUploadWrapper.classList.remove("file-uploaded")
messageInput.dispatchEvent(new Event("input"));





//////////////////////////////////
//create and display user message
//////////////////////////////////





const messageContent = `<div class="message-text"></div>
${userData.file.data ? `<img src="${userData.file.data}" class="attachment" />` : ""}`;


const outgoingMessageDiv= createMessageElement(messageContent, "user-message");
outgoingMessageDiv.querySelector(".message-text").textContent=userData.message;
chatBody.appendChild(outgoingMessageDiv);
chatBody.scrollTo({top: chatBody.scrollHeight, behavior: "smooth"});

let botmessageContent;
console.log(user_id_forMode)
console.log(userId)
console.log(client_id_forMode)
console.log("BOTMODE ", botMode)
console.log("Client Code: ", client_code)

if  (botMode == 'automatic' || overallMode == 'automatic'){
// <svg class="bot-avatar" xmlns="http://www.w3.org/2000/svg" width="50" height="50" 
// viewBox="0 0 1024 1024">
//   <path d="M738.3 287.6H285.7c-59 0-106.8 47.8-106.8 106.8v303.1c0 59 47.8 106.8 106.8 106.8h81.5v111.1c0 .7.8 1.1 1.4.7l166.9-110.6 41.8-.8h117.4l43.6-.4c59 0 106.8-47.8 106.8-106.8V394.5c0-59-47.8-106.9-106.8-106.9zM351.7 448.2c0-29.5 23.9-53.5 53.5-53.5s53.5 23.9 53.5 53.5-23.9 53.5-53.5 53.5-53.5-23.9-53.5-53.5zm157.9 267.1c-67.8 0-123.8-47.5-132.3-109h264.6c-8.6 61.5-64.5 109-132.3 109zm110-213.7c-29.5 0-53.5-23.9-53.5-53.5s23.9-53.5 53.5-53.5 53.5 23.9 53.5 53.5-23.9 53.5-53.5 53.5zM867.2 644.5V453.1h26.5c19.4 0 35.1 15.7 35.1 35.1v121.1c0 19.4-15.7 35.1-35.1 35.1h-26.5zM95.2 609.4V488.2c0-19.4 15.7-35.1 35.1-35.1h26.5v191.3h-26.5c-19.4 0-35.1-15.7-35.1-35.1zM561.5 149.6c0 23.4-15.6 43.3-36.9 49.7v44.9h-30v-44.9c-21.4-6.5-36.9-26.3-36.9-49.7 0-28.6 23.3-51.9 51.9-51.9s51.9 23.3 51.9 51.9z" />
//     </svg>
botmessageContent=`
<div class="bot-avatar" style="color: white">
<span class="material-symbols-outlined graph-icon">graph_6</span>
</div>
  <div class="message-text">
    <div class="thinking-indicator">
      <div class="dot"></div>
      <div class="dot"></div>
      <div class="dot"></div>
    </div>
  </div>`;

}
// if (user_id_forMode === userId && client_id_forMode === client_code && (botMode == 'manual' || overallMode == 'manual')){
  if (botMode == 'manual' || overallMode == 'manual'){


    const existingTyping = chatBody.querySelector(
    `.message.bot-message.thinking[data-user-id="${userId}"]`
  );
  
  if (existingTyping) {
    existingTyping.remove();
  }

botmessageContent=`
<div class="human-avatar">
  <span class="material-symbols-outlined" style="font-size: 32px; color: white;">
    support_agent
  </span>
</div>



<div class="manual-typing-indicator" style="font-style: italic; font-size: 12.5px">Sales colleague is typing </div>
<div class="message-text">
    <div class="thinking-indicator">
      <div class="dot"></div>
      <div class="dot"></div>
      <div class="dot"></div>
    </div>
  </div>`;
}
// Create a placeholder for bot response (typing indicator)


const incomingMessageDiv= createMessageElement(botmessageContent, "bot-message", "thinking");
incomingMessageDiv.dataset.userId = userId;

chatBody.appendChild(incomingMessageDiv);
chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });
// Store the bot message div reference so it can be updated later




}





messageInput.addEventListener("keydown", (e)=>{
const userMessage=e.target.value.trim();
if(e.key==="Enter" && userMessage && !e.shiftKey && window.innerWidth>768){
handleOutgoingMessage(e);
}
});



messageInput.addEventListener("input", () =>{
messageInput.style.height=`${initalInputHeight}px`;
messageInput.style.height=`${messageInput.scrollHeight}px`;
document.querySelector(".chat-form").style.borderRadius=messageInput.scrollHeight>initalInputHeight ? "15px":"32px";
})


const picker = new EmojiMart.Picker({
theme: "light",
skinTonePosition:"none",
previewPosition: "none",
onEmojiSelect:(emoji)=>{
const {selectionStart: start, selectionEnd: end}=messageInput;
messageInput.setRangeText(emoji.native, start, end, "end")
messageInput.focus();
},
onClickOutside:(e) => {
if(e.target.id==="emoji-picker"){
document.body.classList.toggle("show-emoji-picker");
}else{
document.body.classList.remove("show-emoji-picker");
}
}

})


// Shared handler function (language aware)
    function handleAgentRequest(userMessage, botMessage) {

      // Lock input if automatic mode
      if (botMode === 'automatic' || overallMode === 'automatic') {
        lockInput();
      }
      const confirmPanel = document.getElementById("human-confirm");
      confirmPanel.classList.add("hidden");

      // --- 1. Display user message ---
      const userMsgDiv = createMessageElement(`<div class="message-text"></div>`, "user-message");
      userMsgDiv.querySelector(".message-text").textContent = userMessage;
      chatBody.appendChild(userMsgDiv);
      chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });

      // --- 2. Display bot “connecting” message ---
      const botMsgHTML = `
        <div class="bot-avatar" style="color: white;">
          <span class="material-symbols-outlined graph-icon">graph_6</span>
        </div>
        <div class="message-text">
          ${botMessage}
          <div class="thinking-indicator" style="margin-top: 16px;">
            <div class="dot"></div>
            <div class="dot"></div>
            <div class="dot"></div>
          </div>
        </div>
      `;

      const botThinkingDiv = createMessageElement(botMsgHTML, "bot-message", "thinking");
      botThinkingDiv.dataset.userId = userId;
      chatBody.appendChild(botThinkingDiv);
      chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });
      
      // Keep reference so we can later remove it
      window.lastThinkingDiv = botThinkingDiv;

  
    }





document.querySelector(".chat-form").appendChild(picker)
sendMessageButton.addEventListener("click", (e) => handleOutgoingMessage(e))



const fileUploadBtn = document.querySelector("#file-upload");

const wrapper = document.querySelector(".file-upload-wrapper");

// Update class based on mode
function updateUploadMode() {
  if (botMode === 'automatic' || overallMode === 'automatic') {
    wrapper.classList.add("automatic");
  } else {
    wrapper.classList.remove("automatic");
  }
}

fileUploadBtn.addEventListener("click", () => {
  if (botMode === 'automatic' || overallMode === 'automatic') {
    // Prevent file picker in automatic mode
    return;
  }

  fileInput.click();
});

// Call once when mode changes
updateUploadMode();



document.addEventListener("DOMContentLoaded", () => {
  document.body.classList.add("show-chatbot");
  const chatbotToggler = document.querySelector("#chatbot-toggler");
  const closeChatbot = document.querySelector("#close-chatbot");

  if (chatbotToggler) {
    chatbotToggler.addEventListener("click", () => {
      const isOpen = document.body.classList.toggle("show-chatbot");
    
      // .toggle() does two things at the same time: It adds OR removes the class And it RETURNS a value (true or false)
      const message = { chatbotOpen: isOpen };
    
      // Dynamically get the parent origin
      const parentOrigin = window.location !== window.parent.location 
        ? document.referrer 
        : window.location.origin;

      window.parent.postMessage(message, parentOrigin);
    });
  }

    const askHumanBtn = document.getElementById("ask-human");
    const confirmPanel = document.getElementById("human-confirm");
   
    const closeConfirm = document.getElementById("close-confirm");

    // Only one language: just show first message immediately
    const firstMessage = document.getElementById("first-bot-message");
    const messageText = firstMessage.querySelector(".message-text");
    firstMessage.style.display = "flex";

    const lang = selectedLanguage;
    messageText.textContent = lang === "hu"
        ? "Üdvözöllek! Miben tudok segíteni?"
        : "Welcome! How can I help you?";

    updateUIText(lang);
    firstMessage.scrollIntoView({ behavior: "smooth" });

    // Toggle visibility of Ask Human icon
    messageInput.addEventListener("input", () => {
      askHumanBtn.style.display = messageInput.value.trim().length > 0 ? "none" : "inline-block";
    });

    // Show confirmation popup when clicked
    askHumanBtn.addEventListener("click", () => {
      confirmPanel.classList.toggle("hidden");
    });

    
   

    // Close confirmation panel
    closeConfirm.addEventListener("click", () => {
      confirmPanel.classList.add("hidden");
    });

    // Hide panel when clicking outside it

   


  if (closeChatbot) {
    closeChatbot.addEventListener("click", () => {
      document.body.classList.remove("show-chatbot");
      const message = { chatbotOpen: false };
     
      const parentOrigin = window.location !== window.parent.location 
      ? document.referrer 
      : window.location.origin;

      window.parent.postMessage(message, parentOrigin);
    });
  }
});




const selectHU = document.getElementById("select-hu");
const selectEN = document.getElementById("select-en");
const languageSelector = document.getElementById("language-selector");
const chatMessages = document.getElementById("chat-messages");


let selectedLanguage = supportedLanguages[0];








function updateUIText(lang) {
  const t = translations[lang];

  document.getElementById("human-confirm-text").textContent = t.humanConfirmText;
  document.querySelector(".confirm-label").textContent = t.confirmYes;

  document.querySelector(".logo-text").textContent = t.logo_text;
  document.querySelector(".message-input").placeholder = t.sendPlaceholder;
}


// function showHumanConfirm(lang) {
//   const popup = document.getElementById("human-confirm");
//   const textEl = document.getElementById("human-confirm-text");
//   const btnHU = document.getElementById("confirm-yes-hu");
//   const btnEN = document.getElementById("confirm-yes-en");

//   // show popup
//   popup.classList.remove("hidden");

//   if (lang === "hu") {
//     textEl.textContent = "Szeretne ügyintézővel beszélni?"; //always appears on the left, because your CSS makes the popup display: flex
//     btnHU.style.display = "inline-flex";   // show Hungarian button
//     btnEN.style.display = "none";          // hide English button
//   } else {
//     textEl.textContent = "Would you like to talk to a colleague?";
//     btnHU.style.display = "none";          // hide Hungarian button
//     btnEN.style.display = "inline-flex";   // show English button
//   }
// }













// PRIMARY COLOR ELEMENTS (your original names kept exactly)

const solidPicker = document.getElementById("solid-picker");
const gradientPicker = document.getElementById("gradient-picker");
const solidInput = document.getElementById("solid-color");
const gradientStart = document.getElementById("gradient-start");
const gradientEnd = document.getElementById("gradient-end");
const applyBtn = document.getElementById("apply-colors");
const modeRadios = document.querySelectorAll('input[name="color-mode"]');






// REPLY BG COLOR ELEMENTS (your original names kept exactly)

const replySolidPicker = document.getElementById("reply-solid-picker");
const replyGradientPicker = document.getElementById("reply-gradient-picker");
const replySolidInput = document.getElementById("reply-solid-color");
const replyGradientStart = document.getElementById("reply-gradient-start");
const replyGradientEnd = document.getElementById("reply-gradient-end");
const applyReplyBtn = document.getElementById("apply-reply-color");
const replyModeRadios = document.querySelectorAll('input[name="reply-mode"]');



// popup bg elements

const popupSolidPicker = document.getElementById("popup-solid-picker");
const popupGradientPicker = document.getElementById("popup-gradient-picker");
const popupSolidInput = document.getElementById("popup-solid-color");
const popupGradientStart = document.getElementById("popup-gradient-start");
const popupGradientEnd = document.getElementById("popup-gradient-end");
const applyPopupBtn = document.getElementById("apply-popup-bg");
const popupModeRadios = document.querySelectorAll('input[name="popup-bg-mode"]');

// Footer BG elements

const footerSolidPicker = document.getElementById("footer-solid-picker");
const footerGradientPicker = document.getElementById("footer-gradient-picker");
const footerSolidInput = document.getElementById("footer-solid-color");
const footerGradientStart = document.getElementById("footer-gradient-start");
const footerGradientEnd = document.getElementById("footer-gradient-end");
const applyFooterBtn = document.getElementById("apply-footer-bg");
const footerModeRadios = document.querySelectorAll('input[name="footer-bg-mode"]');

// Footer Controls elements
const footerControlsSolidPicker = document.getElementById("footer-controls-solid-picker");
const footerControlsGradientPicker = document.getElementById("footer-controls-gradient-picker");
const footerControlsSolidInput = document.getElementById("footer-controls-solid-color");
const footerControlsGradientStart = document.getElementById("footer-controls-gradient-start");
const footerControlsGradientEnd = document.getElementById("footer-controls-gradient-end");
const applyFooterControlsBtn = document.getElementById("apply-footer-controls");
const footerControlsRadios = document.querySelectorAll('input[name="footer-controls-mode"]');

// Footer Input Text Background elements
const footerInputSolidPicker = document.getElementById("footer-input-solid-picker");
const footerInputGradientPicker = document.getElementById("footer-input-gradient-picker");
const footerInputSolidInput = document.getElementById("footer-input-solid-color");
const footerInputGradientStart = document.getElementById("footer-input-gradient-start");
const footerInputGradientEnd = document.getElementById("footer-input-gradient-end");
const applyFooterInputBtn = document.getElementById("apply-footer-input-bg");
const footerInputModeRadios = document.querySelectorAll('input[name="footer-input-bg-mode"]');


// Popup border elements
const popupBorderColorInput = document.getElementById("popup-border-color");
const popupBorderRadiusInput = document.getElementById("popup-border-radius");
const popupBorderRadiusValue = document.getElementById("popup-border-radius-value");
const popupBorderWidthInput = document.getElementById("popup-border-width");
const popupBorderWidthValue = document.getElementById("popup-border-width-value");
const applyPopupBorderBtn = document.getElementById("apply-popup-border");

// BUBBLE (bot avatar + user message) picker
const bubbleSolidPicker = document.getElementById("bubble-solid-picker");
const bubbleGradientPicker = document.getElementById("bubble-gradient-picker");
const bubbleSolidInput = document.getElementById("bubble-solid-color");
const bubbleGradientStart = document.getElementById("bubble-gradient-start");
const bubbleGradientEnd = document.getElementById("bubble-gradient-end");
const bubbleModeRadios = document.querySelectorAll('input[name="bubble-mode"]');
const applyBubbleBtn = document.getElementById("apply-bubble-color");


// OPERATOR ICON COLOR ELEMENTS (solid only)

const operatorSolidInput = document.getElementById("operator-icon-color");
const applyOperatorBtn = document.getElementById("apply-operator-icon");

// FONT COLOR ELEMENTS (solid only)

const fontColorInput = document.getElementById("font-color-input");
const applyFontColorBtn = document.getElementById("apply-font-color");


// BOT FONT COLOR ELEMENTS (solid only)

const everythingWhiteInput = document.getElementById("everything-white-input");
const applyEverythingWhiteBtn = document.getElementById("apply-everything-white");


// MESSAGE INPUT TEXT COLOR ELEMENTS (solid only)

const userInputColor=document.getElementById("user-input-message-color")
const applyInputColor=document.getElementById("apply-user-input-message-color")


// Footer focus outline elements
const footerFocusInput = document.getElementById("footer-focus-outline-color");
const applyFooterFocusBtn = document.getElementById("apply-footer-focus-outline");


// footer outline not focuesed
const footerOutlineInput = document.getElementById("footer-outline-color");
const applyFooterOutlineBtn = document.getElementById("apply-footer-outline");

// scrollbar
const scrollbarColorPicker = document.getElementById("scrollbar-color-picker");
const applyScrollbarBtn = document.getElementById("apply-scrollbar-color");

// confirmation button
const confirmationbuttonColorPicker = document.getElementById("confirmation-color-picker");
const applyConfirmationBtn = document.getElementById("apply-confirmation-color");


// ====================================================================
// GENERIC INITIALIZER
// ====================================================================
function initColorPicker(defaultRadioSelector, solidBox, gradientBox) {
  const radio = document.querySelector(defaultRadioSelector);
  if (radio) radio.checked = true;

  solidBox.style.display = "block";
  gradientBox.style.display = "none";
}

function initPickerFromCSS(cssVar, solidBox, gradientBox, solidInput, gStart, gEnd, radioNodeList) {
    const raw = getComputedStyle(document.documentElement)  // document.documentElement This is the root <html> element. That’s where all your CSS variables like --primary-color live.
                    .getPropertyValue(cssVar)
                    .trim();

    if (!raw) return; // nothing to initialize

    if (raw.startsWith("linear-gradient")) {
        // ▸ Gradient
        const colors = raw.match(/#([0-9a-fA-F]{3,6})/g);

        if (colors && colors.length >= 2) {
            gStart.value = colors[0];
            gEnd.value = colors[1];
        }

        solidBox.style.display = "none";
        gradientBox.style.display = "flex";

        // set radio
        radioNodeList.forEach(r => r.checked = r.value === "gradient");

    } else {
        // ▸ Solid
        solidInput.value = raw;

        solidBox.style.display = "flex";
        gradientBox.style.display = "none";

        radioNodeList.forEach(r => r.checked = r.value === "solid");
    }
}

function initBubblePicker(solidBox, gradientBox, solidInput, gStart, gEnd, radioNodeList) {
    // Get computed style of the bot avatar background
    let raw = getComputedStyle(document.documentElement)
                .getPropertyValue('--bot-avatar-bg')
                .trim();

    // If it's a var(), resolve it
    if (raw.startsWith('var(')) {
        const innerVar = raw.match(/var\((--[^)]+)\)/)[1]; // e.g., --primary-color
        raw = getComputedStyle(document.documentElement)
                .getPropertyValue(innerVar)
                .trim();
    }

    if (!raw) return;

    if (raw.startsWith("linear-gradient")) {
        const colors = raw.match(/#([0-9a-fA-F]{3,6})/g);

        if (colors && colors.length >= 2) {
            gStart.value = colors[0];
            gEnd.value = colors[1];
        }

        solidBox.style.display = "none";
        gradientBox.style.display = "flex";

        radioNodeList.forEach(r => r.checked = r.value === "gradient");

    } else {
        solidInput.value = raw;

        solidBox.style.display = "flex";
        gradientBox.style.display = "none";

        radioNodeList.forEach(r => r.checked = r.value === "solid");
    }
}




// ====================================================================
// GENERIC TOGGLER (for primary & reply colors only) shows/hides the correct picker
// ====================================================================
function setupModeToggle(radioNodeList, solidBox, gradientBox) {
  radioNodeList.forEach(radio => {
    radio.addEventListener("change", () => {
      if (radio.value === "solid") {
        solidBox.style.display = "block";
        gradientBox.style.display = "none";
      }else if(radio.value === "primary") {
        solidBox.style.display = "none";
        gradientBox.style.display = "none";
      } else {
        solidBox.style.display = "none";
        gradientBox.style.display = "block";
      }
    });
  });
}

// ====================================================================
//  APPLY HANDLERS (for solid + gradient)
 // ====================================================================
function setupApplyButton(button, radioSelector, solidInput, gStart, gEnd, cssVarName) {
  button.addEventListener("click", () => {
    const mode = document.querySelector(radioSelector + ':checked').value;
    let value;

    if (mode === "solid") {
        value = solidInput.value;
    } else {
        // vertical gradient for popup, horizontal for others
        const direction = (cssVarName === '--popup-bg-color') ? 'to bottom' : 'to right';
        value = `linear-gradient(${direction}, ${gStart.value}, ${gEnd.value})`;
    }


    // Set the main CSS variable
    document.documentElement.style.setProperty(cssVarName, value);

    // If this is the primary color, also update the first-color variable
    if (cssVarName === '--primary-color') {
      let firstColor;
      if (value.startsWith('linear-gradient')) {
        const match = value.match(/#([0-9a-fA-F]{3,6})/);
        if (match) firstColor = match[0];
      } else {
        firstColor = value;
      }

      if (firstColor) {
        document.documentElement.style.setProperty('--primary-color-first-color', firstColor);
      }
    }
  });
}

function setupMultiApplyButton(button, radioSelector, solidInput, gStart, gEnd, cssVarList) {
  button.addEventListener("click", () => {
    const mode = document.querySelector(radioSelector).value;
    let value;

    if (mode === "primary") {
      // Use the primary color directly
      value = `var(--primary-color)`;
    } 
    else if (mode === "solid") {
      value = solidInput.value;
    } 
    else {
      // gradient mode
      value = `linear-gradient(to right, ${gStart.value}, ${gEnd.value})`;
    }

    cssVarList.forEach(cssVar => {
      document.documentElement.style.setProperty(cssVar, value);
    });
  });
}



// ====================================================================
// RUN EVERYTHING ON LOAD
// ====================================================================
window.addEventListener("DOMContentLoaded", () => {

  // PRIMARY COLOR PICKER INIT (először a solid colorpickert választja)
  initColorPicker(
    'input[name="color-mode"][value="solid"]',
    solidPicker,
    gradientPicker
  );

  initPickerFromCSS(
    '--primary-color',
    solidPicker,
    gradientPicker,
    solidInput,
    gradientStart,
    gradientEnd,
    modeRadios
);

  

  // REPLY BG PICKER INIT
  initPickerFromCSS(
  '--reply-bg-color',
  replySolidPicker,
  replyGradientPicker,
  replySolidInput,
  replyGradientStart,
  replyGradientEnd,
  replyModeRadios
);

  // POPUP BG PICKER INIT
  initPickerFromCSS(
  '--popup-bg-color',
  popupSolidPicker,
  popupGradientPicker,
  popupSolidInput,
  popupGradientStart,
  popupGradientEnd,
  popupModeRadios
);

  // FOOTER BG PICKER INIT
  initColorPicker(
      '--footer-bg-color',
    footerSolidPicker,
    footerGradientPicker,
    footerSolidInput,
    footerGradientStart,
    footerGradientEnd,
    footerModeRadios
  );
  
  // FOOTER BG CONTROLS INIT
  initColorPicker(
     '--footer-controls-bg',
    footerControlsSolidPicker,
    footerControlsGradientPicker,
    footerControlsSolidInput,
    footerControlsGradientStart,
    footerControlsGradientEnd,
    footerControlsRadios
  );

  // FOOTER BG INPUT TEXT INIT
  initColorPicker(
    '--footer-input-bg-color',
      footerInputSolidPicker,
      footerInputGradientPicker,
      footerInputSolidInput,
      footerInputGradientStart,
      footerInputGradientEnd,
      footerInputModeRadios
  );

  // INIT BUBBLE PICKER (bot avatar + user message)
  initBubblePicker(
    bubbleSolidPicker,
    bubbleGradientPicker,
    bubbleSolidInput,
    bubbleGradientStart,
    bubbleGradientEnd,
    bubbleModeRadios
);

  


  ///////////////////////
  // PRIMARY COLOR TOGGLE
  ///////////////////////

  setupModeToggle(
    modeRadios,
    solidPicker,
    gradientPicker
  );

  // REPLY BG COLOR TOGGLE
  setupModeToggle(
    replyModeRadios,
    replySolidPicker,
    replyGradientPicker
  );

  // POPUP BG COLOR TOGGLE
  setupModeToggle(
    popupModeRadios,
    popupSolidPicker,
    popupGradientPicker
  );
  // Toggle solid/gradient footer
  setupModeToggle(
  footerModeRadios,
  footerSolidPicker,
  footerGradientPicker
);

  // Toggle solid/gradient footer controls
  setupModeToggle(
    footerControlsRadios,
    footerControlsSolidPicker,
    footerControlsGradientPicker
  );

  // Toggle solid/gradient footer imput text bg
  setupModeToggle(
    footerInputModeRadios,
    footerInputSolidPicker,
    footerInputGradientPicker
  );

  // TOGGLE BUBBLE PICKER (bot avatar + user message)
  setupModeToggle(
    bubbleModeRadios,
    bubbleSolidPicker,
    bubbleGradientPicker
  );


  // APPLY PRIMARY COLOR
  setupApplyButton(
    applyBtn,
    'input[name="color-mode"]:checked',
    solidInput,
    gradientStart,
    gradientEnd,
    '--primary-color'
  );

  // APPLY REPLY BG COLOR
  setupApplyButton(
    applyReplyBtn,
    'input[name="reply-mode"]:checked',
    replySolidInput,
    replyGradientStart,
    replyGradientEnd,
    '--reply-bg-color'
  );

  // Popup BG (vertical gradient)
  setupApplyButton(
    applyPopupBtn,
    'input[name="popup-bg-mode"]:checked',
    popupSolidInput,
    popupGradientStart,
    popupGradientEnd,
    '--popup-bg-color'
  );
   // Apply color/gradient footer
  setupApplyButton(
    applyFooterBtn,
    'input[name="footer-bg-mode"]:checked',
    footerSolidInput,
    footerGradientStart,
    footerGradientEnd,
    '--footer-bg-color'
  );

  // Apply color/gradient footer controls
  setupApplyButton(
    applyFooterControlsBtn,
    'input[name="footer-controls-mode"]:checked',
    footerControlsSolidInput,
    footerControlsGradientStart,
    footerControlsGradientEnd,
    '--footer-controls-bg'
  );

   // Apply color/gradient footer text input bg
  setupApplyButton(
    applyFooterInputBtn,
    'input[name="footer-input-bg-mode"]:checked',
    footerInputSolidInput,
    footerInputGradientStart,
    footerInputGradientEnd,
    '--footer-input-bg-color'
  );

  setupMultiApplyButton(
    applyBubbleBtn,
    'input[name="bubble-mode"]:checked',
    bubbleSolidInput,
    bubbleGradientStart,
    bubbleGradientEnd,
    ['--bot-avatar-bg', '--user-message-bg']
  );

  // APPLY OPERATOR ICON COLOR (solid only)
  applyOperatorBtn.addEventListener("click", () => {
    document.documentElement.style.setProperty('--operator-icon', operatorSolidInput.value);
  });

  // APPLY FONT COLOR (solid only)
  applyFontColorBtn.addEventListener("click", () => {
    const value = fontColorInput.value;

    // Set CSS variable
    document.documentElement.style.setProperty('--font-color', value);

    // Update specific elements directly if needed
    document.querySelectorAll('#confirm-yes-hu, #confirm-yes-en').forEach(el => {
      el.style.color = value;
    });
  });
  // APPLY BOT FONT COLOR (solid only) Bot text mindenféle más kütyü
  applyEverythingWhiteBtn.addEventListener("click", () => {
    const value = everythingWhiteInput.value;
    document.documentElement.style.setProperty('--everything-which-is-white', value);
  });

  applyInputColor.addEventListener("click", () => {
    const value = userInputColor.value;
    document.documentElement.style.setProperty('--user-input-message-color', value);
  });

  // Apply color footer focus
  applyFooterFocusBtn.addEventListener("click", () => {
    const color = footerFocusInput.value;
    document.documentElement.style.setProperty('--footer-focus-outline-color', color);
  });

  // Update live text for sliders
  popupBorderRadiusInput.addEventListener("input", () => {
    popupBorderRadiusValue.textContent = popupBorderRadiusInput.value + "px";
  });

  popupBorderWidthInput.addEventListener("input", () => {
    popupBorderWidthValue.textContent = popupBorderWidthInput.value + "px";
  });

  //Footer outline not focued
      applyFooterOutlineBtn.addEventListener("click", () => {
        const color = footerOutlineInput.value;
        document.documentElement.style.setProperty('--footer-form-outline', color);
      });

      // Scrollbar
      applyScrollbarBtn.addEventListener("click", () => {
          const color = scrollbarColorPicker.value;
          document.documentElement.style.setProperty("--scrollbar-color", color);
      });
  


       // Confirmation button
      applyConfirmationBtn.addEventListener("click", () => {
          const color = confirmationbuttonColorPicker.value;
          document.documentElement.style.setProperty("--confirmation-button-bgcolor", color);
      });
    

  // Apply button
  applyPopupBorderBtn.addEventListener("click", () => {
    const color = popupBorderColorInput.value;
    const radius = popupBorderRadiusInput.value + "px";
    const width = popupBorderWidthInput.value + "px";

    document.documentElement.style.setProperty("--border-color", color);
    document.documentElement.style.setProperty("--border-radius", radius);
    document.documentElement.style.setProperty("--border-width", width);
  });

  function initBotAvatarPicker() {
      const solidPicker = document.getElementById("bot-avatar-solid-picker");
      const gradientPicker = document.getElementById("bot-avatar-gradient-picker");

      const solidInput = document.getElementById("bot-avatar-solid");
      const gradientStart = document.getElementById("bot-avatar-gradient-start");
      const gradientEnd = document.getElementById("bot-avatar-gradient-end");

      const modeRadios = document.querySelectorAll("input[name='bot-avatar-mode']");
      const applyBtn = document.getElementById("apply-bot-avatar-bg");

      // Read current css variable
      const currentValue = getComputedStyle(document.documentElement)
                              .getPropertyValue('--bot-avatar-bg')
                              .trim();

      // Detect gradient
      if (currentValue.startsWith("linear-gradient")) {
          const colors = currentValue.match(/#([0-9a-fA-F]{3,6})/g);

          if (colors && colors.length >= 2) {
              gradientStart.value = colors[0];
              gradientEnd.value = colors[1];
          }

          solidPicker.style.display = "none";
          gradientPicker.style.display = "flex";
          document.querySelector("input[value='gradient']").checked = true;

      } else {
          solidInput.value = currentValue || "#5061C4";
          solidPicker.style.display = "flex";
          gradientPicker.style.display = "none";
          document.querySelector("input[value='solid']").checked = true;
      }

      // Switch UI on mode change
      modeRadios.forEach(radio => {
          radio.addEventListener("change", () => {
              if (radio.value === "solid") {
                  solidPicker.style.display = "flex";
                  gradientPicker.style.display = "none";
              } else {
                  solidPicker.style.display = "none";
                  gradientPicker.style.display = "flex";
              }
          });
      });

      // Apply selected colors
      applyBtn.addEventListener("click", () => {
          const mode = document.querySelector("input[name='bot-avatar-mode']:checked").value;

          if (mode === "solid") {
              const color = solidInput.value;
              document.documentElement.style.setProperty("--bot-avatar-bg", color);
          } else {
              const g1 = gradientStart.value;
              const g2 = gradientEnd.value;

              const gradient = `linear-gradient(90deg, ${g1}, ${g2})`;
              document.documentElement.style.setProperty("--bot-avatar-bg", gradient);
          }
      });
      
      
    
    }

    


     
  

  window.addEventListener("DOMContentLoaded", initBotAvatarPicker);

  let idleTimeout = 60 * 60 * 1000; // 1 hour
  let lastActivity = Date.now();

  function resetActivity() {
      lastActivity = Date.now();
  }

  // Capture activity
  document.addEventListener('mousemove', resetActivity);
  document.addEventListener('keydown', resetActivity);
  document.addEventListener('scroll', resetActivity);

  // Heartbeat function
  async function sendHeartbeat() {
    const now = Date.now();
    if (now - lastActivity < idleTimeout) {
        try {
            const resp = await fetch('/heartbeat', { method: 'POST', credentials: 'include' });
            //Tells the browser to include cookies from the current site in the request, even for cross-origin requests.
            // Body: You don’t send a body here, so nothing is explicitly sent in the request payload.
            console.log("[Heartbeat] sent", resp.status);
        } catch (e) {
            console.error("[Heartbeat] failed", e);
        }
    } else {
        console.log("[Heartbeat] idle, not sending");
    }
}

  // Send heartbeat every 1 min
  setInterval(sendHeartbeat, 60 * 1000);





 

});

