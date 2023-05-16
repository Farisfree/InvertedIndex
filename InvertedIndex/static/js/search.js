// 搜索按钮点击事件
document.getElementById("search-button").addEventListener("click", function() {
    var searchInput = document.getElementById("search-input").value;
    var searchMode = document.getElementById("search-mode-button").textContent;

    // 根据搜索方式进行搜索逻辑处理

    // 更新搜索结果展示和分页

});

// 切换搜索方式按钮点击事件
document.getElementById("search-mode-button").addEventListener("click", function() {
    var searchModeButton = document.getElementById("search-mode-button");
    if (searchModeButton.textContent === "搜索方式1") {
        searchModeButton.textContent = "搜索方式2";
    } else {
        searchModeButton.textContent = "搜索方式1";
    }
});

// 更新热度排行榜
function updateHotList(hotKeywords) {
    var hotList = document.getElementById("hot-list");
    hotList.innerHTML = "";

    for (var i = 0; i < hotKeywords.length; i++) {
        var listItem = document.createElement("li");
        listItem.textContent = hotKeywords[i];
        hotList.appendChild(listItem);
    }
}
