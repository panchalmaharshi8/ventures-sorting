(function () {
  // =========================
  // Helpers
  // =========================
  function $(id) {
    const el = document.getElementById(id);
    if (!el) throw new Error(`Missing element with id="${id}"`);
    return el;
  }

  function show(el) {
    el.classList.remove("hidden");
  }

  function hide(el) {
    el.classList.add("hidden");
  }

  // System log container (scrolling log area)
  function getLogContainer() {
    return document.querySelector(".custom-scrollbar");
  }

  function addLogLine(message, color = "text-slate-600", border = "border-l-2 border-slate-300") {
    const logContainer = getLogContainer();
    if (!logContainer) return;

    const ts = new Date().toLocaleTimeString([], { hour12: false });
    const row = document.createElement("div");
    row.className = `flex gap-3 ${border} pl-3`;
    row.innerHTML = `
      <span class="text-slate-400 select-none">[${ts}]</span>
      <span class="${color}">${message}</span>
    `;
    logContainer.appendChild(row);
    logContainer.scrollTop = logContainer.scrollHeight;
  }

  // Download helper
  function triggerDownload(url) {
    // Use a normal navigation download (works well with Flask send_file)
    const a = document.createElement("a");
    a.href = url;
    a.rel = "noopener";
    a.target = "_blank";
    document.body.appendChild(a);
    a.click();
    a.remove();
  }

  // =========================
  // Feature: Folder Input
  // =========================
  function initFolderInput() {
    const folderInput = $("folderInput");
    const selectedPath = $("selectedPath");
    const pathText = $("pathText");
    const clearBtn = $("clearSelectionBtn");

    folderInput.addEventListener("change", (e) => {
      const files = e.target.files;
      if (!files || files.length === 0) return;

      const folderName = (files[0].webkitRelativePath || "").split("/")[0] || "Selected Folder";
      pathText.textContent = `Selected: ${folderName} (${files.length} files)`;

      show(selectedPath);
      selectedPath.classList.add("animate-fade-in-up");
    });

    clearBtn.addEventListener("click", () => {
      folderInput.value = "";
      hide(selectedPath);
    });
  }

  // =========================
  // Feature: Target Schema => OMOP options visibility
  // =========================
  function initFormatChange() {
    const toFormat = $("toFormat");
    const omopOptions = $("omopOptions");

    function apply() {
      if (toFormat.value === "omop") show(omopOptions);
      else hide(omopOptions);
    }

    toFormat.addEventListener("change", apply);
    apply();
  }

  // =========================
  // Feature: Manual Override accordion
  // =========================
  function initOverrideToggle() {
    const btn = $("toggleOverrideBtn");
    const panel = $("overridePanel");
    const icon = $("overrideIcon");

    btn.addEventListener("click", () => {
      const isHidden = panel.classList.contains("hidden");
      if (isHidden) {
        show(panel);
        icon.classList.add("rotate-90");
      } else {
        hide(panel);
        icon.classList.remove("rotate-90");
      }
    });
  }

  // =========================
  // Feature: Anonymization enables Convert
  // =========================
  function initAnonymizeCheck() {
    const check = $("anonymizeCheck");
    const btn = $("convertBtn");

    function apply() {
      if (check.checked) {
        btn.disabled = false;
        btn.classList.remove("opacity-50", "cursor-not-allowed", "grayscale");
        btn.classList.add("animate-pulse-once");
      } else {
        btn.disabled = true;
        btn.classList.add("opacity-50", "cursor-not-allowed", "grayscale");
        btn.classList.remove("animate-pulse-once");
      }
    }

    check.addEventListener("change", apply);
    apply();
  }

  // =========================
  // Feature: Modal open/close
  // =========================
  function openModal(id) {
    const modal = $(id);
    const content = modal.querySelector("#modalContent");
    if (!content) throw new Error("Modal missing #modalContent");

    modal.classList.remove("hidden");

    setTimeout(() => {
      modal.classList.remove("opacity-0");
      content.classList.remove("scale-95");
      content.classList.add("scale-100");
    }, 10);
  }

  function closeModal(id) {
    const modal = $(id);
    const content = modal.querySelector("#modalContent");
    if (!content) throw new Error("Modal missing #modalContent");

    modal.classList.add("opacity-0");
    content.classList.remove("scale-100");
    content.classList.add("scale-95");

    setTimeout(() => {
      modal.classList.add("hidden");
    }, 300);
  }

  function initModal() {
    const openBtn = $("viewDroppedFieldsBtn");
    const closeBtn = $("closeModalBtn");
    const ackBtn = $("ackCloseBtn");

    openBtn.addEventListener("click", () => openModal("nonTransferableModal"));
    closeBtn.addEventListener("click", () => closeModal("nonTransferableModal"));
    ackBtn.addEventListener("click", () => closeModal("nonTransferableModal"));

    const modal = $("nonTransferableModal");
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeModal("nonTransferableModal");
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !modal.classList.contains("hidden")) {
        closeModal("nonTransferableModal");
      }
    });
  }

  // =========================
  // Feature: Convert + Download
  // =========================
  function initConvertAndDownload() {
    const convertBtn = $("convertBtn");
    const downloadBtn = $("downloadBtn");
    const toFormat = $("toFormat");
    const folderInput = $("folderInput");

    // Track last job download URL
    let lastDownloadUrl = null;

    function setDownloadEnabled(enabled) {
      if (!downloadBtn) return;
      if (enabled) {
        downloadBtn.disabled = false;
        downloadBtn.classList.remove("opacity-50", "cursor-not-allowed", "grayscale");
      } else {
        downloadBtn.disabled = true;
        downloadBtn.classList.add("opacity-50", "cursor-not-allowed", "grayscale");
      }
    }

    // start disabled
    setDownloadEnabled(false);

    downloadBtn.addEventListener("click", async () => {
      if (!lastDownloadUrl) {
        addLogLine("No output available to download for the last job.", "text-amber-700", "border-l-2 border-amber-500");
        return;
      }
      addLogLine("Downloading output artifact...", "text-blue-600", "border-l-2 border-[#00A9CE]");
      triggerDownload(lastDownloadUrl);
    });

    convertBtn.addEventListener("click", async () => {
      const target = toFormat.value === "fhir" ? "fhir" : "omop";

      const files = folderInput?.files ? Array.from(folderInput.files) : [];
      if (!files.length) {
        addLogLine("No files selected. Please choose a folder first.", "text-red-600", "border-l-2 border-red-500");
        return;
      }

      // reset last download state
      lastDownloadUrl = null;
      setDownloadEnabled(false);

      addLogLine(`Submitting conversion request (target=${target})...`, "text-blue-600", "border-l-2 border-[#00A9CE]");
      addLogLine(`Uploading ${files.length} file(s)...`, "text-blue-600", "border-l-2 border-[#00A9CE]");

      try {
        const form = new FormData();
        form.append("target", target);
        for (const file of files) {
          form.append("files", file, file.name);
        }

        const res = await fetch("/api/convert", { method: "POST", body: form });
        const data = await res.json();

        if (!res.ok || !data.ok) {
          const msg = data?.etl?.message ? data.etl.message : JSON.stringify(data);
          addLogLine(`Conversion failed: ${msg}`, "text-red-600", "border-l-2 border-red-500");

          // Explicit: no output
          addLogLine("No output artifact was generated for this job.", "text-amber-700", "border-l-2 border-amber-500");
          return;
        }

        addLogLine(`ETL success: ${data.etl.message}`, "text-green-600", "border-l-2 border-green-500");

        const dl = data?.artifacts?.download_url || null;
        if (dl) {
          lastDownloadUrl = dl;
          setDownloadEnabled(true);
          addLogLine("Output ready. Click “Download Report” to download the ZIP.", "text-green-600", "border-l-2 border-green-500");
        } else {
          addLogLine("ETL succeeded but no downloadable output artifact was found.", "text-amber-700", "border-l-2 border-amber-500");
        }
      } catch (err) {
        addLogLine(`Request failed: ${err?.message || String(err)}`, "text-red-600", "border-l-2 border-red-500");
        addLogLine("No output artifact was generated for this job.", "text-amber-700", "border-l-2 border-amber-500");
      }
    });
  }

  // =========================
  // Boot
  // =========================
  document.addEventListener("DOMContentLoaded", () => {
    initFolderInput();
    initFormatChange();
    initOverrideToggle();
    initAnonymizeCheck();
    initModal();
    initConvertAndDownload();
  });
})();
