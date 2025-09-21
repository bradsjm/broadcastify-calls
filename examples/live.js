var apiParms = {};
var pos;
var receivedFirstCalls = false;
var tableData = [];
var callsTable;
var calls = [];
var callPos = 0;
var highCallSet = false;
var lowCall = 0;
var highCall = 0;
var currCallIdx = 0;
var nextClipId = 0;
var holdingCall = 0;
var overRuns = 0;
var playing = false;
var playInterval;
var playingCallId;
var lastPlayedCall;
var userStopped = false;
var started = false;
var allowedToStart = false;
var callAudioPlayer;
var gainValue = 0;
var doInit = 1;
var clickedCall = false;
var holdClicked = false;
var requestingData = false;
var speed = 1.0;
var tags = {
  1: { tagDescr: "Multi-Dispatch" },
  2: { tagDescr: "Law Dispatch" },
  3: { tagDescr: "Fire Dispatch" },
  4: { tagDescr: "EMS Dispatch" },
  6: { tagDescr: "Multi-Tac" },
  7: { tagDescr: "Law Tac" },
  8: { tagDescr: "Fire-Tac" },
  9: { tagDescr: "EMS-Tac" },
  11: { tagDescr: "Interop" },
  12: { tagDescr: "Hospital" },
  13: { tagDescr: "Ham" },
  14: { tagDescr: "Public Works" },
  15: { tagDescr: "Aircraft" },
  16: { tagDescr: "Federal" },
  17: { tagDescr: "Business" },
  20: { tagDescr: "Railroad" },
  21: { tagDescr: "Other" },
  22: { tagDescr: "Multi-Talk" },
  23: { tagDescr: "Law Talk" },
  24: { tagDescr: "Fire-Talk" },
  25: { tagDescr: "EMS-Talk" },
  26: { tagDescr: "Transportation" },
  29: { tagDescr: "Emergency Ops" },
  30: { tagDescr: "Military" },
  31: { tagDescr: "Media" },
  32: { tagDescr: "Schools" },
  33: { tagDescr: "Security" },
  34: { tagDescr: "Utilities" },
  35: { tagDescr: "Data" },
  36: { tagDescr: "Deprecated" },
  37: { tagDescr: "Corrections" },
};
var MSG_livewaiting = "Live - Waiting for call . . .";

function supportedBrowser() {
  //var iOS = /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
  if (isIOS()) {
    return false;
  } else {
    return true;
  }
}

function isIOS() {
  if (/iPad|iPhone|iPod/.test(navigator.platform)) {
    return true;
  } else {
    return (
      navigator.maxTouchPoints &&
      navigator.maxTouchPoints > 2 &&
      /MacIntel/.test(navigator.platform)
    );
  }
}

function toggleMute() {
  if (trace) console.log("Mute called");
  if (callAudioPlayer.volume == 0) {
    if (trace) console.log("Currently current muted, setting to " + gainValue);
    callAudioPlayer.volume = gainValue;
    $("#muteicon")
      .removeClass("fas fa-volume-mute")
      .addClass("fa fa-volume-up");
    $("#mutebutton").removeClass("btn-warning").addClass("btn-soft-secondary");
  } else if (gainValue.volume != 0) {
    if (trace)
      console.log("Volume is " + callAudioPlayer.volume + " - Now Muting");
    gainValue = callAudioPlayer.volume;
    callAudioPlayer.volume = 0;
    $("#muteicon")
      .removeClass("fa fa-volume-up")
      .addClass("fas fa-volume-mute");
    $("#mutebutton").removeClass("btn-soft-secondary").addClass("btn-warning");
  }
}

function updateLabels(id) {
  if (id) {
    if (trace) console.log("Update Labels For: " + id);
    var callIdx = calls
      .map(function (o) {
        return o.id;
      })
      .indexOf(id);

    if (calls[callIdx].attrs === undefined) {
      $("#grpId, #grp_descr, #call_src, #grouping, #tagName, #freq").html("");
      return 0;
    }

    var callDisplay = calls[callIdx].attrs.display
      ? calls[callIdx].attrs.display
      : "Unknown";
    var callDescr = calls[callIdx].attrs.descr
      ? calls[callIdx].attrs.descr
      : "Unknown";
    var freq = calls[callIdx].attrs.call_freq
      ? calls[callIdx].attrs.call_freq
      : "Unknown";

    $("#statusField")
      .removeClass("text-light text-success")
      .addClass("text-primary");
    $("#statusField").html(callDisplay);
    document.title = callDisplay;

    $("#grpId").html(calls[callIdx].attrs.tg);
    $("#grp_descr").html(callDescr);
    $("#freq").html(freq);
    $("#call_src").html(calls[callIdx].attrs.call_src);
    $("#grouping").html(calls[callIdx].attrs.grouping);
    $("#tagName").html(calls[callIdx].attrs.tag);
  } else {
    if (userStopped) {
      $("#statusField")
        .removeClass("text-primary text-success")
        .addClass("text-light");
      $("#statusField").html("Stopped");
      document.title = "Stopped";
    } else {
      $("#statusField")
        .removeClass("text-primary text-light")
        .addClass("text-success");
      if (holdingCall) {
        $("#statusField").html("Live - Holding: " + holdingCall.attrs.display);
        document.title = "Holding: " + holdingCall.attrs.display;
      } else {
        $("#statusField").html(MSG_livewaiting);
        document.title = "Live - Waiting";
      }
    }
    $("#grpId, #grp_descr, #call_src, #grouping, #tagName, #freq").html("");
  }
}

$(document).ready(function () {
  $.ajaxSetup({ cache: false });

  var supported = supportedBrowser();

  if (supported) {
    $("#volumeRow").removeClass("d-none").addClass("d-inline-block");
    $("#pannerRow").removeClass("d-none").addClass("d-inline-block");
    $("#mutebutton").removeClass("d-none").addClass("d-inline-block");
  }

  if (playlist) {
    var emptyTableMessage = "No Calls Seen in the Past 10 Minutes";
  } else {
    var emptyTableMessage = "No Calls Available";
  }

  callAudioPlayer = document.getElementById("call_audio_player");

  // Create the Audio Context for the Player
  var panControl = document.querySelector("#panning-control");
  var panValue = document.querySelector("#panning-value");

  if (supported && panControl) {
    var audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    var sourceNode = audioCtx.createMediaElementSource(callAudioPlayer);
    var balanceNode = new StereoBalanceNode(audioCtx);
    sourceNode.connect(balanceNode);
    balanceNode.connect(audioCtx.destination);

    balanceNode.balance.value = panControl.value;
    // Event handler function to increase panning to the right and left
    // when the slider is moved
    panControl.oninput = function () {
      balanceNode.balance.value = panControl.value;
      if (panControl.value == 0) {
        panValue.innerHTML = "[ ]";
      } else {
        if (panControl.value > 0) {
          panValue.innerHTML = "R" + Math.abs(panControl.value) * 10;
        } else if (panControl.value < 0) {
          panValue.innerHTML = "L" + Math.abs(panControl.value) * 10;
        }
      }
    };
    callAudioPlayer.addEventListener("play", function () {
      audioCtx.resume();
    });
  }

  $("#call_audio_player").bind("playing", function (e) {
    $("#btn-" + playingCallId)
      .removeClass("fa fa-play")
      .addClass("fa fa-stop");
    $("#call-" + playingCallId).addClass("table-success");
    updateLabels(playingCallId);
    playing = true;
  });

  $("#call_audio_player").bind("pause", function (e) {
    if (trace) console.log("End/Pause: " + playingCallId);
    $("#btn-" + playingCallId)
      .removeClass("fa fa-stop")
      .addClass("fa fa-play");
    $("#call-" + playingCallId).removeClass("table-success");
    updateLabels(false);
    playing = false;
    playingCallId = 0;
  });
  $("#call_audio_player").bind("error", function (e) {
    $("#btn-" + playingCallId)
      .removeClass("fa fa-stop")
      .addClass("fa fa-play");
    $("#call-" + playingCallId).removeClass("table-success");
    //playing = false;
    updateLabels(false);
    playingCallId = 0;
  });
  $("#call_audio_player").bind("ended", function (e) {
    $("#btn-" + playingCallId)
      .removeClass("fa fa-stop")
      .addClass("fa fa-play");
    $("#call-" + playingCallId).removeClass("table-success");
    //playing = false;
    playingCallId = 0;
    updateLabels(false);
    if (trace) console.log("Call Ended. userStopped:" + userStopped);
    if (!userStopped) {
      playNextClip();
    }
  });

  var gainControl = document.querySelector("#gain");
  var speedControl = document.querySelector("#speed");

  if (trace) console.log("Setting initial gain: " + gainControl.value);
  callAudioPlayer.volume = gainControl.value;

  gainControl.addEventListener("change", function () {
    callAudioPlayer.volume = gainControl.value;
    if (trace) console.log("Overall Gain: " + callAudioPlayer.volume);
  });
  gainControl.addEventListener("input", function () {
    callAudioPlayer.volume = gainControl.value;
    if (trace) console.log("Overall Gain: " + callAudioPlayer.volume);
  });

  if (speedControl) {
    speedControl.addEventListener("change", function () {
      callAudioPlayer.playbackRate = speedControl.value;
      speed = speedControl.value;
      $("#speedLabel").html(speedControl.value);
      if (trace) console.log("Overall Speed: " + callAudioPlayer.playbackRate);
    });
    speedControl.addEventListener("input", function () {
      callAudioPlayer.playbackRate = speedControl.value;
      speed = speedControl.value;
      $("#speedLabel").html(speedControl.value);
      if (trace) console.log("Overall Speed: " + callAudioPlayer.playbackRate);
    });
  }

  callsTable = $("#callsTable").DataTable({
    autoWidth: false,
    columns: [
      { width: "15px", render: callTableColumnPlay },
      { width: "100px", render: callTableColumnTime },
      { width: "80px", render: callTableColumnTG },
      { width: "90px", render: callTableColumnSRC },
      { width: "30px", render: $.fn.dataTable.render.text() },
      { width: "150px", render: callTableColumnDisplay },
      { render: callTableColumnDescr },
      { width: "30px", render: callTableColumnActions },
    ],
    columnDefs: [
      { targets: [0], visible: play },
      { targets: [1], className: "dt-nowrap", visible: true },
      {
        targets: [2],
        className: "d-none d-sm-table-cell dt-nowrap",
        visible: showTg,
      },
      {
        targets: [3],
        className: "d-none d-sm-table-cell dt-nowrap",
        visible: showSrc,
      },
      { targets: [4], visible: true },
      { targets: [5], visible: showDisplay },
      { targets: [6], className: "d-none d-sm-table-cell", visible: showDescr },
      { targets: [7], className: "dt-nowrap", visible: true },
    ],
    paging: false,
    ordering: false,
    searching: false,
    processing: true,
    info: false,
    createdRow: function (row, data, index) {
      $(row).addClass("cursor-link");
      $(row).attr("id", "call-" + data[0]);
      if (!highCallSet) {
        highCall = data[0];
        highCallSet = true;
      }
      // Last Created Row is the low call
      lowCall = data[0];
      if (playingCallId == data[0]) {
        $(row).addClass("table-success");
      } else {
        $(row).removeClass("table-success");
      }
    },
    language: {
      processing: "Loading...",
      emptyTable: emptyTableMessage,
    },
  });

  if (!pos) {
    pos = Math.floor(Date.now() / 1000) - startQueueSeconds;
  }
  getLatestData();
});

var callTableColumnTime = function (data, type, row) {
  var dateObj = new Date(parseInt(data) * 1000);
  var dateString = dateObj.toLocaleTimeString();
  if (!isToday(dateObj)) {
    dateString +=
      '<br /><span class="small text-muted">' +
      dateObj.toLocaleDateString() +
      "</span>";
  }
  return dateString;
};
var callTableColumnTG = function (data, type, row) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(data);
  var call_type = parseInt(calls[callIdx].attrs.call_type);
  if (call_type == 3 || call_type == 4 || call_type == 5) {
    var sid = parseInt(calls[callIdx].attrs.sid);
    var tg = parseInt(calls[callIdx].attrs.tg);
    var key = calls[callIdx].attrs.key;
    var columnString =
      '<a class="text-info" href="/calls/tg/' +
      sid +
      "/" +
      tg +
      '" data-toggle="modal" ' +
      'data-target="#grpActionsModal" data-value="' +
      key +
      '">' +
      tg +
      "</a>";
  } else if (call_type == 1 || call_type == 2) {
    var fid = parseInt(calls[callIdx].attrs.fid);
    var freq = calls[callIdx].attrs.call_freq;
    var key = calls[callIdx].attrs.key;
    var columnString =
      '<a class="text-info" href="/calls/fid/' +
      fid +
      '" data-toggle="modal" ' +
      'data-target="#grpActionsModal" data-value="' +
      key +
      '">' +
      freq.toFixed(4) +
      "</a>";
  }
  if (playlist) {
    columnString +=
      '<i onClick="holdGrp(' +
      data +
      ')" class="fas fa-compress-arrows-alt text-warning hold-tg ml-1"></i>';
  }
  return columnString;
};
var callTableColumnSRC = function (data, type, row) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(data);
  var call_type = parseInt(calls[callIdx].attrs.call_type);
  if (call_type == 3 || call_type == 4 || call_type == 5) {
    var sid = parseInt(calls[callIdx].attrs.sid);
    var srcId = parseInt(calls[callIdx].attrs.call_src);
    var src_descr = calls[callIdx].attrs.call_src_descr;
    if (srcId > 0) {
      var columnString =
        '<a class="text-info" href="/calls/src/' +
        sid +
        "/" +
        srcId +
        '">' +
        srcId +
        "</a>";
    } else {
      var columnString = srcId;
    }
    if (src_descr) {
      columnString +=
        '<br /><span class="small text-muted">' + src_descr + "</span>";
    }
  } else if (call_type == 1 || call_type == 2) {
    var columnString = "0";
  }
  return columnString;
};

var callTableColumnDisplay = function (data, type, row) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(data);
  var columnString = calls[callIdx].attrs.display;
  var tagId = calls[callIdx].attrs.tag;

  if (tagId && !playlist) {
    columnString +=
      '<br /><span class="small text-muted">' +
      tags[tagId].tagDescr +
      "</span>";
  }

  return columnString;
};

var callTableColumnDescr = function (data, type, row) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(data);
  var columnString = calls[callIdx].attrs.descr;
  var grouping = calls[callIdx].attrs.grouping;

  if (grouping && !playlist) {
    columnString +=
      '<br /><span class="small text-muted">' + grouping + "</span>";
  }
  return columnString;
};

var callTableColumnPlay = function (data, type, row) {
  if (playingCallId == data) {
    var iconClass = "fa-stop";
  } else {
    var iconClass = "fa-play";
  }
  var columnString =
    '<a class="callAudioEntry" data-value="' +
    data +
    '"><span id="btn-' +
    data +
    '" class="fa ' +
    iconClass +
    '" role="status"></span></a>';
  return columnString;
};
var callTableColumnActions = function (data, type, row) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(data);
  var systemId = calls[callIdx].attrs.systemId;
  var filename = calls[callIdx].attrs.filename;
  var enc = calls[callIdx].attrs.enc;
  var hash = calls[callIdx].attrs.hash;
  var columnString = '<span class="small">';
  columnString +=
    '<a data-toggle="modal" data-target="#callDetailsModal" data-value="' +
    data +
    '"><span class="fa fa-search" role="status"></span></a>';
  if (typeof hash !== "undefined") {
    columnString +=
      '<a href="https://calls.broadcastify.com/' +
      hash +
      "/" +
      systemId +
      "/" +
      filename +
      "." +
      enc +
      '" class="text-dark ml-2" data-value="' +
      data +
      '">';
  } else {
    columnString +=
      '<a href="https://calls.broadcastify.com/' +
      systemId +
      "/" +
      filename +
      "." +
      enc +
      '" class="text-dark ml-2" data-value="' +
      data +
      '">';
  }
  columnString += '<span class="fa fa-download" role="status"></span></a>';
  columnString += "</span>";
  return columnString;
};

function handleNewCalls(data) {
  requestingData = false;
  let obj = null;
  try {
    obj = JSON.parse(data);
  } catch (e) {
    obj = data;
  }

  var newCallsCount = obj.calls.length;
  doInit = 0;
  //if (trace) console.log("New Calls Received: " + newCallsCount);
  if (!receivedFirstCalls && newCallsCount) {
    receivedFirstCalls = true;
  }
  highCallSet = false;
  for (i = 0; i < newCallsCount; i++) {
    callPos++;
    // console.log("Setting Call Position: " + callPos);
    // obj.calls[i].call_src_descr ? obj.calls[i].call_src_descr : '',

    var call_type = obj.calls[i].call_type;

    var groupId;
    if (call_type == 3 || call_type == 4 || call_type == 5) {
      groupId = call_type + "-" + obj.calls[i].sid + "-" + obj.calls[i].call_tg;
    } else if (call_type == 1 || call_type == 2) {
      groupId =
        call_type +
        "-" +
        obj.calls[i].fid +
        "-" +
        obj.calls[i].call_freq.toFixed(4);
    }

    var srcId;
    if (call_type == 3 || call_type == 4 || call_type == 5) {
      srcId = call_type + "-" + obj.calls[i].sid + "-" + obj.calls[i].call_src;
    } else if (call_type == 1 || call_type == 2) {
      srcId = call_type + "-" + obj.calls[i].fid + "-" + 0;
    }

    var callDisplay = obj.calls[i].display ? obj.calls[i].display : "";
    var callDescr = obj.calls[i].descr ? obj.calls[i].descr : "";

    var callEntry = [
      callPos,
      obj.calls[i].meta_starttime,
      callPos,
      callPos,
      obj.calls[i].call_duration,
      callPos,
      callPos,
      callPos,
    ];
    tableData.unshift(callEntry);
    calls.unshift({
      id: callPos,
      attrs: {
        key: obj.calls[i].id,
        systemId: obj.calls[i].systemId,
        groupId: groupId,
        sid: obj.calls[i].sid ? obj.calls[i].sid : 0,
        fid: obj.calls[i].fid ? obj.calls[i].fid : 0,
        call_type: obj.calls[i].call_type,
        tg: obj.calls[i].call_tg,
        call_src: obj.calls[i].call_src,
        call_src_descr: obj.calls[i].call_src_descr
          ? obj.calls[i].call_src_descr
          : "",
        call_freq: obj.calls[i].call_freq ? obj.calls[i].call_freq : 0,
        descr: obj.calls[i].descr ? obj.calls[i].descr : "",
        display: obj.calls[i].display ? obj.calls[i].display : "",
        grouping: obj.calls[i].grouping ? obj.calls[i].grouping : "",
        tag: obj.calls[i].tag,
        filename: obj.calls[i].filename,
        ts: obj.calls[i].ts,
        hash: obj.calls[i].hash,
        enc: obj.calls[i].enc ? obj.calls[i].enc : "m4a",
      },
    });
  }
  if (tableData.length > maxCalls) {
    tableData.length = maxCalls;
    calls.length = maxCalls;
  }
  //if (trace) console.log('Last POS: ' + obj.lastPos);
  if (obj.lastPos) {
    pos = obj.lastPos + 1;
  }
  if (newCallsCount) {
    callsTable.clear();
    callsTable.rows.add(tableData);
    callsTable.draw();
    $(".callAudioEntry").click(function () {
      clickedCall = true;
      var entryPoint = $(this).attr("data-value");
      var callId = parseInt(entryPoint);
      if (!started) {
        $("#btn-" + callId)
          .removeClass("fa fa-play")
          .addClass("fa fa-stop");
      }
      if (trace) console.log("CallId: " + callId);
      var callIdx = calls
        .map(function (o) {
          return o.id;
        })
        .indexOf(callId);
      if (trace) console.log("Call IDX " + callIdx);
      var filename = calls[callIdx].attrs.filename;
      var systemId = calls[callIdx].attrs.systemId;
      if (trace) console.log("Playing: " + playing);
      if (playing) {
        userStopped = true;
        toogleLive();
        //callAudioPlayer.pause();
      } else {
        userStopped = false;
        currCallIdx = callId;
        //playCall(callId);
        toogleLive();
      }
    });
  }
  if (!playing) {
    startPlaying();
  }
  //if (trace) console.log('Low Call: ' + lowCall + ' High Call: ' + highCall);
}

function getLatestData() {
  requestingData = true;
  if (playlist) {
    apiParms = {
      groups: groups,
      pos: pos,
      doInit: doInit,
      systemId: systemId,
      sid: sid,
      playlist_uuid: playlist_uuid,
      sessionKey: sessionKey,
    };
  } else {
    apiParms = {
      groups: groups,
      pos: pos,
      doInit: doInit,
      systemId: systemId,
      sid: sid,
      sessionKey: sessionKey,
    };
  }

  $.ajax({
    url: apiEndpoint,
    type: "post",
    data: apiParms,
  })
    .done(function (data) {
      handleNewCalls(data);
    })
    .always(function () {
      window.setTimeout(getLatestData, getNewCallsInterval * 1000);
    });
  /*
    $.ajax({
        url: apiEndpoint,
        type: 'post',
        data: apiParms,
        success: function( data, textStatus, jQxhr ){
            handleNewCalls(data)
        },
        error: function( jqXhr, textStatus, errorThrown ){
            console.log('ERROR:' + errorThrown );
        }
    });
     */
}
function playNextClip() {
  if (!currCallIdx) {
    nextClipId = highCall;
  } else if (currCallIdx < lowCall) {
    nextClipId = lowCall;
    overRuns++;
  } else {
    if (clickedCall) {
      clickedCall = false;
      nextClipId = currCallIdx;
      if (trace)
        console.log(
          "PlayNextClip Clicked Call is True, nextClipId=" + currCallIdx
        );
    } else {
      if (!holdingCall) {
        if (trace) console.log("Ran nextClipId = currCallIdx+1");
        nextClipId = currCallIdx + 1;
      }
    }
  }

  if (nextClipId > highCall || !receivedFirstCalls) {
    playing = false;
    playingCallId = 0;
  } else {
    if (trace)
      console.log(
        "Next Clip ID: " + nextClipId + " currCallIdx: " + currCallIdx
      );
    if (holdingCall) {
      for (var i = calls.length - 1; i >= 0; i--) {
        var groupId = calls[i].attrs.groupId;
        var callId = calls[i].id;
        if (holdingCall.attrs.groupId == groupId) {
          if (
            (holdClicked && callId >= nextClipId) ||
            (callId > nextClipId && callId != lastPlayedCall)
          ) {
            if (trace)
              console.log("callId: " + callId + " currCallIdx: " + currCallIdx);
            playing = true;
            if (trace) console.log("-->HOLD Playing Next ID: " + nextClipId);
            playCall(callId);
            playingCallId = callId;
            if (holdClicked) {
              holdClicked = false;
              nextClipId = callId;
            } else {
              nextClipId = callId + 1;
            }
            currCallIdx = callId;

            $("#call-" + playingCallId).addClass("table-success");
            break;
          }
        } else {
          //if (trace) console.log("Holding.. skip: " + callId);
        }
      }
    } else {
      playing = true;
      if (trace) console.log("-->Playing Next ID: " + nextClipId);
      playCall(nextClipId);
      currCallIdx = nextClipId;
      playingCallId = currCallIdx;
      $("#call-" + currCallIdx).addClass("table-success");
    }
  }
}
function playCall(callId) {
  callAudioPlayer.pause();
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(callId);
  playingCallId = callId;
  lastPlayedCall = callId;
  var filename = calls[callIdx].attrs.filename;
  var systemId = calls[callIdx].attrs.systemId;
  var enc = calls[callIdx].attrs.enc;
  var hash = calls[callIdx].attrs.hash;
  if (typeof hash !== "undefined") {
    callAudioPlayer.src =
      "https://calls.broadcastify.com/" +
      hash +
      "/" +
      systemId +
      "/" +
      filename +
      "." +
      enc;
  } else {
    callAudioPlayer.src =
      "https://calls.broadcastify.com/" + systemId + "/" + filename + "." + enc;
  }
  callAudioPlayer.playbackRate = speed;
  callAudioPlayer.play().catch(function (e) {
    console.log("Unable to play audio clip: " + e);
    playNextClip();
  });
}

function skipCall() {
  if (playing) {
    $("#btn-" + playingCallId)
      .removeClass("fa fa-stop")
      .addClass("fa fa-play");
    $("#call-" + playingCallId).removeClass("table-success");
    playNextClip();
  }
}
function prevCall() {
  if (playing) {
    if (holdingCall) {
      for (i = 0; i < calls.length; i++) {
        var groupId = calls[i].attrs.groupId;
        var callId = calls[i].id;
        var prevClipId;
        if (holdingCall.attrs.groupId == groupId) {
          prevClipId = callId;
          if (prevClipId < currCallIdx) {
            $("#btn-" + playingCallId)
              .removeClass("fa fa-stop")
              .addClass("fa fa-play");
            $("#call-" + playingCallId).removeClass("table-success");
            playing = true;
            if (trace) console.log("-->Playing Prev ID: " + prevClipId);
            playCall(prevClipId);
            currCallIdx = prevClipId;
            playingCallId = currCallIdx;
            $("#call-" + currCallIdx).addClass("table-success");
            break;
          }
        } else {
          if (trace) console.log("Holding.. skip non hold: " + callId);
        }
      }
    } else {
      $("#btn-" + playingCallId)
        .removeClass("fa fa-stop")
        .addClass("fa fa-play");
      $("#call-" + playingCallId).removeClass("table-success");
      currCallIdx = playingCallId - 1;
      playCall(playingCallId - 1);
    }
  } else {
    if (lastPlayedCall) {
      playCall(lastPlayedCall);
      currCallIdx = lastPlayedCall;
    }
  }
}

function startPlaying() {
  if (allowedToStart && !playing) {
    if (holdingCall) {
      //currCallIdx = currCallIdx-1;
      playNextClip();
    } else {
      playNextClip();
    }
  }
}

function toogleLive() {
  if (!started) {
    started = true;
    allowedToStart = true;
    userStopped = false;
    $("#stateicon").removeClass("fa fa-play");
    $("#stateicon").addClass("fa fa-stop");

    $("#statebutton").removeClass("btn-soft-success");
    $("#statebutton").addClass("btn-soft-primary");
    $("#playStop").html("Stop");

    $("#statusField")
      .removeClass("text-primary text-light")
      .addClass("text-success");
    $("#statusField").html(MSG_livewaiting);
    document.title = "Live - Waiting";
    startPlaying();
  } else {
    started = false;
    callAudioPlayer.pause();
    allowedToStart = false;
    userStopped = true;
    currCallIdx = 0;
    holdingCall = 0;
    $("#stateicon").removeClass("fa fa-stop");
    $("#stateicon").addClass("fa fa-play");

    $("#statebutton").removeClass("btn-soft-primary");
    $("#statebutton").addClass("btn-soft-success");
    $("#playStop").html("Play");
    $("#statusField").removeClass("text-primary").addClass("text-light");
    $("#statusField").html("Stopped");
    document.title = "Stopped";
    $("#holdButton").removeClass("btn-warning").addClass("btn-soft-secondary");
  }
}

function toggleHold() {
  if (holdingCall) {
    holdingCall = 0;
    currCallIdx = highCall;
    $("#statusField").html(MSG_livewaiting);
    document.title = "Live - Waiting";
    $("#holdButton").removeClass("btn-warning").addClass("btn-soft-secondary");
    $("#holdButton")
      .tooltip()
      .attr("data-original-title", "Hold Current Group");
    if (trace) console.log("Clearing Hold");
  } else {
    var callIdx = calls
      .map(function (o) {
        return o.id;
      })
      .indexOf(playingCallId);
    $("#statusField")
      .removeClass("text-primary text-light")
      .addClass("text-success");
    $("#statusField").html("Live - Holding: " + calls[callIdx].attrs.display);
    document.title = "Live - Holding:" + calls[callIdx].attrs.display;
    $("#holdButton").removeClass("btn-soft-secondary").addClass("btn-warning");
    $("#holdButton")
      .tooltip()
      .attr("data-original-title", "Remove Current Hold");
    holdingCall = calls[callIdx];
    if (trace)
      console.log(
        "Holding on playing call ID: " +
          holdingCall.id +
          " Group ID: " +
          holdingCall.attrs.groupId
      );
  }
}

function holdGrp(id) {
  var callIdx = calls
    .map(function (o) {
      return o.id;
    })
    .indexOf(id);
  $("#statusField")
    .removeClass("text-primary text-light")
    .addClass("text-success");
  $("#statusField").html("Live - Holding: " + calls[callIdx].attrs.display);
  $("#holdButton").removeClass("btn-soft-secondary").addClass("btn-warning");
  $("#holdButton").tooltip().attr("data-original-title", "Remove Current Hold");
  holdingCall = calls[callIdx];
  currCallIdx = id;
  if (trace)
    console.log(
      "Holding on call ID: " +
        holdingCall.id +
        " Group ID: " +
        holdingCall.attrs.groupId
    );
  clickedCall = true;
  holdClicked = true;
  if (!started) {
    toogleLive();
  } else {
    $("#btn-" + playingCallId)
      .removeClass("fa fa-stop")
      .addClass("fa fa-play");
    $("#call-" + playingCallId).removeClass("table-success");
    playCall(id);
  }
}

const isToday = (someDate) => {
  const today = new Date();
  return (
    someDate.getDate() == today.getDate() &&
    someDate.getMonth() == today.getMonth() &&
    someDate.getFullYear() == today.getFullYear()
  );
};

function StereoBalanceNode(context, options = { balance: 0 }) {
  let balance = 0;

  const upMixer = context.createGain();
  upMixer.channelCount = 2;
  upMixer.channelCountMode = "explicit";
  upMixer.channelInterpretation = "speakers";

  const splitter = context.createChannelSplitter(2);

  // Create the gains for left and right
  const leftGain = context.createGain();
  const rightGain = context.createGain();

  const merger = context.createChannelMerger(2);

  upMixer.connect(splitter);

  splitter.connect(leftGain, 0);
  splitter.connect(rightGain, 1);

  leftGain.connect(merger, 0, 0);
  rightGain.connect(merger, 0, 1);

  // -1 (left) to 1 (right)
  function set(rawValue) {
    const value = Number(rawValue);
    leftGain.gain.value = value > 0 ? 1 - value : 1;
    rightGain.gain.value = value > 0 ? 1 : 1 + value;
    balance = value;
  }

  function get() {
    return balance;
  }

  const audioParam = {};
  Object.defineProperties(audioParam, {
    value: { get, set, enumerable: true, configurable: true },
  });

  Object.defineProperties(upMixer, {
    balance: {
      value: audioParam,
      enumerable: true,
      writable: false,
      configurable: true,
    },
    connect: {
      value: AudioNode.prototype.connect.bind(merger),
      enumerable: false,
      writable: false,
      configurable: true,
    },
    disconnect: {
      value: AudioNode.prototype.disconnect.bind(merger),
      enumerable: false,
      writable: false,
      configurable: true,
    },
  });

  if (balance !== options.balance) {
    set(options.balance);
  }

  return upMixer;
}
