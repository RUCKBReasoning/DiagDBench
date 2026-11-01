/** Compatibility facade over the result managers used by the application. */
import { createResultOperationsManager } from '../../managers/result-operations-manager.js';
import { createCopyUtilsManager } from '../../managers/copy-utils-manager.js';
import { createUIDisplayManager } from '../../managers/ui-display-manager.js';

export function createResultActions(popup) {
    const operations = createResultOperationsManager(popup);
    const copy = createCopyUtilsManager(popup);
    const display = createUIDisplayManager(popup);
    return {
        showLoadingOverlay: display.showLoadingOverlay.bind(display),
        hideLoadingOverlay: display.hideLoadingOverlay.bind(display),
        copyResult: operations.copyResult.bind(operations),
        exportResultAsHtml: operations.exportResultAsHtml.bind(operations),
        clearResult: operations.clearResult.bind(operations),
        copyQuestionText: copy.copyQuestionText.bind(copy),
        fallbackCopyTextToClipboard: copy.fallbackCopyTextToClipboard.bind(copy),
        showCopySuccess: copy.showCopySuccess.bind(copy)
    };
}
