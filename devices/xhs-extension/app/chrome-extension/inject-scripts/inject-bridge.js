/* eslint-disable */
/**
 * Injection Bridge Script
 *
 * This script acts as a communication bridge between:
 * - ISOLATED world (Content Script environment)
 * - MAIN world (Page JavaScript environment)
 *
 * It enables the extension to execute code in the page's JavaScript context
 * and retrieve results back to the extension.
 */

(() => {
  // Prevent duplicate injection
  if (window.__COAGENT_XHS_INJECT_BRIDGE_LOADED__) return;
  window.__COAGENT_XHS_INJECT_BRIDGE_LOADED__ = true;

  // Event names for communication
  const EVENT_NAME = {
    EXECUTE: 'coagent-xhs:execute',
    RESPONSE: 'coagent-xhs:response',
    CLEANUP: 'coagent-xhs:cleanup',
  };

  // Store pending requests
  const pendingRequests = new Map();

  /**
   * Handle messages from the extension background script
   */
  const messageHandler = (request, _sender, sendResponse) => {
    // Handle cleanup command
    if (request.type === EVENT_NAME.CLEANUP) {
      window.dispatchEvent(new CustomEvent(EVENT_NAME.CLEANUP));
      sendResponse({ success: true });
      return true;
    }

    // Handle script execution for MAIN world
    if (request.targetWorld === 'MAIN' && request.type === 'EXECUTE_SCRIPT') {
      const requestId = `req-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

      // Store the sendResponse callback
      pendingRequests.set(requestId, sendResponse);

      // Dispatch event to MAIN world
      window.dispatchEvent(
        new CustomEvent(EVENT_NAME.EXECUTE, {
          detail: {
            action: request.action,
            code: request.code,
            args: request.args,
            requestId: requestId,
          },
        })
      );

      // Indicate async response
      return true;
    }
  };

  // Listen for messages from background script
  chrome.runtime.onMessage.addListener(messageHandler);

  /**
   * Handle responses from MAIN world
   */
  const responseHandler = (event) => {
    const { requestId, data, error } = event.detail;

    if (pendingRequests.has(requestId)) {
      const sendResponse = pendingRequests.get(requestId);
      sendResponse({ data, error });
      pendingRequests.delete(requestId);
    }
  };

  // Listen for responses from MAIN world
  window.addEventListener(EVENT_NAME.RESPONSE, responseHandler);

  /**
   * Cleanup handler
   */
  const cleanupHandler = () => {
    // Remove all listeners
    chrome.runtime.onMessage.removeListener(messageHandler);
    window.removeEventListener(EVENT_NAME.RESPONSE, responseHandler);
    window.removeEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

    // Clear pending requests
    pendingRequests.clear();

    // Remove marker
    delete window.__COAGENT_XHS_INJECT_BRIDGE_LOADED__;
  };

  // Listen for cleanup event
  window.addEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

  // Log successful initialization
  console.log('[Coagent XHS] Inject bridge loaded');
})();
