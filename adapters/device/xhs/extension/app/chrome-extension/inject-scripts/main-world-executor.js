/* eslint-disable */
/**
 * Main World Executor
 *
 * This script runs in the MAIN world (page JavaScript context) and:
 * - Listens for execution requests from the inject bridge
 * - Executes JavaScript code in the page context
 * - Returns results back through the event system
 */

(() => {
  // Prevent duplicate injection
  if (window.__COAGENT_XHS_MAIN_EXECUTOR_LOADED__) return;
  window.__COAGENT_XHS_MAIN_EXECUTOR_LOADED__ = true;

  // Event names
  const EVENT_NAME = {
    EXECUTE: 'coagent-xhs:execute',
    RESPONSE: 'coagent-xhs:response',
    CLEANUP: 'coagent-xhs:cleanup',
  };

  /**
   * Execute code and return result
   */
  const executeCode = async (code, args = {}) => {
    try {
      // Create a function with the code
      // Use Function constructor for better error handling
      const fn = new Function(
        'args',
        `
        'use strict';
        ${code}
      `
      );

      // Execute the function with provided arguments
      const result = await fn(args);

      return { success: true, result };
    } catch (error) {
      return {
        success: false,
        error: {
          message: error.message,
          stack: error.stack,
          name: error.name,
        },
      };
    }
  };

  /**
   * Handle execution requests
   */
  const executionHandler = async (event) => {
    const { requestId, code, args, action } = event.detail;

    try {
      let result;

      // Handle different action types
      switch (action) {
        case 'EXECUTE_CODE':
          result = await executeCode(code, args);
          break;

        case 'READ_WINDOW_PROPERTY':
          // Special handler for reading window properties
          const path = args.path;
          let value = window;

          for (const key of path.split('.')) {
            if (key && value) {
              value = value[key];
            }
          }

          result = { success: true, result: value };
          break;

        case 'CALL_FUNCTION':
          // Special handler for calling window functions
          const fnPath = args.functionPath;
          const fnArgs = args.arguments || [];

          let fn = window;
          for (const key of fnPath.split('.')) {
            if (key && fn) {
              fn = fn[key];
            }
          }

          if (typeof fn === 'function') {
            const fnResult = await fn.apply(window, fnArgs);
            result = { success: true, result: fnResult };
          } else {
            result = {
              success: false,
              error: { message: `${fnPath} is not a function` },
            };
          }
          break;

        default:
          // Default to code execution
          result = await executeCode(code, args);
      }

      // Send response back
      window.dispatchEvent(
        new CustomEvent(EVENT_NAME.RESPONSE, {
          detail: {
            requestId,
            data: result,
          },
        })
      );
    } catch (error) {
      // Send error response
      window.dispatchEvent(
        new CustomEvent(EVENT_NAME.RESPONSE, {
          detail: {
            requestId,
            error: {
              message: error.message,
              stack: error.stack,
            },
          },
        })
      );
    }
  };

  // Listen for execution requests
  window.addEventListener(EVENT_NAME.EXECUTE, executionHandler);

  /**
   * Cleanup handler
   */
  const cleanupHandler = () => {
    // Remove listeners
    window.removeEventListener(EVENT_NAME.EXECUTE, executionHandler);
    window.removeEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

    // Remove marker
    delete window.__COAGENT_XHS_MAIN_EXECUTOR_LOADED__;

    console.log('[Coagent XHS] Main world executor cleaned up');
  };

  // Listen for cleanup event
  window.addEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

  // Log successful initialization
  console.log('[Coagent XHS] Main world executor loaded');
})();
