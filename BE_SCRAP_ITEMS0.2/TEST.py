import json
import os
import random

# --- 檔案名稱，用於存儲自訂元素 ---
ELEMENTS_FILE = "prompt_elements.json"

# --- 預設選項 (如果檔案不存在時使用) ---
DEFAULT_ELEMENTS = {
    "STYLES": [
        "photorealistic", "impressionistic", "surreal", "abstract", "cartoonish",
        "anime", "manga", "pixel art", "concept art", "fantasy art", "sci-fi art",
        "cyberpunk", "steampunk", "art deco", "art nouveau", "renaissance painting",
        "baroque", "minimalist", "vaporwave", "gothic", "ukiyo-e"
    ],
    "ARTISTS": [
        "Greg Rutkowski", "Alphonse Mucha", "H.R. Giger", "Hayao Miyazaki",
        "Frida Kahlo", "Salvador Dalí", "Vincent van Gogh", "Claude Monet",
        "trending on ArtStation", "trending on DeviantArt", "Studio Ghibli"
    ],
    "MEDIUMS": [
        "digital painting", "oil painting", "watercolor", "pencil sketch", "charcoal drawing",
        "3D render", "CGI", "photograph", "illustration", "graphic novel", "sculpture",
        "ink drawing", "collage"
    ],
    "LIGHTING": [
        "cinematic lighting", "volumetric lighting", "soft light", "hard light", "studio lighting",
        "natural light", "rim lighting", "backlighting", "dramatic lighting", "neon glow",
        "golden hour", "blue hour", "moonlight", "ambient occlusion"
    ],
    "COLORS": [
        "vibrant colors", "monochromatic", "pastel colors", "neon colors", "earthy tones",
        "warm colors", "cool colors", "black and white", "sepia", "iridescent", "bioluminescent",
        "complementary colors", "analogous colors", "triadic colors"
    ],
    "COMPOSITION": [
        "close-up shot", "medium shot", "full shot", "wide shot", "extreme wide shot",
        "low angle", "high angle", "bird's-eye view", "worm's-eye view", "dutch angle",
        "rule of thirds", "dynamic composition", "symmetrical", "leading lines", "depth of field"
    ],
    "QUALITY_TAGS": [
        "highly detailed", "masterpiece", "best quality", "4k", "8k", "ultra-realistic",
        "sharp focus", "intricate details", "professional", "award-winning", "hyperrealistic"
    ],
    "NEGATIVE_PROMPT_SUGGESTIONS": [
        "ugly", "tiling", "poorly drawn hands", "poorly drawn feet", "poorly drawn face",
        "out of frame", "extra limbs", "disfigured", "deformed", "body out of frame",
        "blurry", "bad anatomy", "blurred", "watermark", "grainy", "signature",
        "cut off", "draft", "low quality", "worst quality", "mutation", "mutated",
        "jpeg artifacts", "text", "error", "missing fingers", "extra digits", "fewer digits"
    ]
}

# --- 全域變數，用於存儲當前載入的元素 ---
current_elements = {}

def load_elements():
    """從 JSON 檔案載入元素，如果檔案不存在則使用預設值並創建檔案。"""
    global current_elements
    if os.path.exists(ELEMENTS_FILE):
        try:
            with open(ELEMENTS_FILE, 'r', encoding='utf-8') as f:
                current_elements = json.load(f)
            # 校驗載入的數據是否包含所有必要的鍵
            for key in DEFAULT_ELEMENTS:
                if key not in current_elements:
                    current_elements[key] = DEFAULT_ELEMENTS[key]
            save_elements() # 如果有補齊的鍵，保存一次
        except (json.JSONDecodeError, TypeError):
            print(f"警告: '{ELEMENTS_FILE}' 檔案格式錯誤或損毀，將使用預設元素並嘗試覆蓋。")
            current_elements = DEFAULT_ELEMENTS.copy()
            save_elements()
    else:
        print(f"提示: 未找到 '{ELEMENTS_FILE}'，將使用預設元素並創建新檔案。")
        current_elements = DEFAULT_ELEMENTS.copy()
        save_elements()

def save_elements():
    """將當前元素儲存到 JSON 檔案。"""
    global current_elements
    try:
        with open(ELEMENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(current_elements, f, ensure_ascii=False, indent=4)
    except IOError:
        print(f"錯誤: 無法寫入元素到檔案 '{ELEMENTS_FILE}'。請檢查權限。")


def get_user_input(prompt_message, default_value="", allow_empty=False):
    """獲取使用者輸入，可選填預設值。"""
    while True:
        user_input = input(f"{prompt_message} (預設: {default_value if default_value else '無'}): ").strip()
        if user_input:
            return user_input
        elif default_value or allow_empty:
            return default_value
        else:
            print("此項目為必填項，請重新輸入。")

def get_choice_from_list(category_name, prompt_message, allow_custom=True, allow_multiple=False, allow_empty=False):
    """讓使用者從列表中選擇，可允許多選或自訂，並可為選擇添加描述。"""
    global current_elements
    choices = current_elements.get(category_name, [])
    
    print(f"\n--- {prompt_message} (來自 '{category_name}' 列表) ---")
    for i, choice in enumerate(choices):
        print(f"{i+1}. {choice}")
    
    custom_option_number = len(choices) + 1
    skip_option_number = custom_option_number + 1 if allow_custom else len(choices) + 1

    if allow_custom:
        print(f"{custom_option_number}. 自訂新的 '{category_name[:-1] if category_name.endswith('S') else category_name}'...")
    if allow_empty:
        print(f"{skip_option_number}. 跳過此項")

    selected_items_with_desc = []
    while True:
        try:
            raw_input_choices = input(f"請選擇編號 (若允許多選請用逗號分隔, e.g., 1,3): ").strip()
            if not raw_input_choices and allow_empty:
                return [] if allow_multiple else ""

            input_choice_indices = [int(c.strip()) - 1 for c in raw_input_choices.split(',')]
            
            temp_items = []
            newly_added_custom_items = []

            for index in input_choice_indices:
                if 0 <= index < len(choices):
                    selected_item = choices[index]
                    # 為已選項目添加描述
                    desc_for_item = input(f"  為 '{selected_item}' 添加具體描述嗎? (例如: '帶有柔和光暈'，留空則不加): ").strip()
                    if desc_for_item:
                        temp_items.append(f"{selected_item}, {desc_for_item}")
                    else:
                        temp_items.append(selected_item)

                elif allow_custom and index == custom_option_number -1 : # 自訂選項
                    custom_value = input(f"請輸入自訂的 '{category_name[:-1] if category_name.endswith('S') else category_name}' 內容: ").strip()
                    if custom_value:
                        temp_items.append(custom_value)
                        # 詢問是否要將新的自訂項目永久添加到列表中
                        save_custom = input(f"  是否要將 '{custom_value}' 保存到 '{category_name}' 列表中供未來使用? (y/n, 預設 n): ").lower()
                        if save_custom == 'y':
                            if custom_value not in current_elements[category_name]:
                                current_elements[category_name].append(custom_value)
                                newly_added_custom_items.append(custom_value)
                            else:
                                print(f"  '{custom_value}' 已存在於列表中。")
                    else:
                        print("自訂內容不能為空。")
                        temp_items = [] # 重置以便重新選擇
                        break


                elif allow_empty and index == skip_option_number - 1:
                    if allow_multiple: temp_items = [] # 清空已選，表示跳過
                    else: return "" # 單選跳過
                    break # 跳出內層循環，準備返回

                else:
                    print("無效的選擇，請重新輸入。")
                    temp_items = [] # 重置，以便重新選擇
                    break
            
            if newly_added_custom_items: # 如果有新項目被添加到主列表
                save_elements()
                print(f"已將新的自訂項目保存到 '{ELEMENTS_FILE}'。")


            if temp_items or (not temp_items and allow_empty and not raw_input_choices):
                if not allow_multiple and len(temp_items) > 1:
                    print("此項僅能單選，請重新選擇。")
                    continue
                return temp_items if allow_multiple else (temp_items[0] if temp_items else "")
            elif not temp_items and not allow_empty:
                print("請至少選擇一項或輸入自訂內容。")

        except ValueError:
            print("輸入無效，請輸入數字編號。")

def manage_elements():
    """管理自訂元素列表。"""
    global current_elements
    while True:
        print("\n--- 管理自訂元素 ---")
        print("您可以查看或向以下列表中添加新項目：")
        categories = list(current_elements.keys())
        if "NEGATIVE_PROMPT_SUGGESTIONS" in categories: # 通常這個不需要用戶頻繁修改核心列表
            categories.remove("NEGATIVE_PROMPT_SUGGESTIONS")

        for i, cat_name in enumerate(categories):
            print(f"{i+1}. {cat_name} (目前有 {len(current_elements[cat_name])} 項)")
        print(f"{len(categories)+1}. 返回主選單")

        try:
            choice = int(input("請選擇要管理的列表編號: ").strip()) -1
            if 0 <= choice < len(categories):
                cat_to_manage = categories[choice]
                print(f"\n--- 管理 '{cat_to_manage}' ---")
                print("目前項目:")
                if not current_elements[cat_to_manage]:
                    print("  (此列表為空)")
                else:
                    for item_idx, item_val in enumerate(current_elements[cat_to_manage]):
                        print(f"  {item_idx+1}. {item_val}")
                
                add_new = input(f"\n要向 '{cat_to_manage}' 添加新項目嗎? (y/n, 預設 n): ").lower()
                if add_new == 'y':
                    new_item_value = input(f"請輸入要添加到 '{cat_to_manage}' 的新項目名稱/描述: ").strip()
                    if new_item_value:
                        if new_item_value not in current_elements[cat_to_manage]:
                            current_elements[cat_to_manage].append(new_item_value)
                            save_elements()
                            print(f"'{new_item_value}' 已成功添加到 '{cat_to_manage}' 並已保存。")
                        else:
                            print(f"'{new_item_value}' 已存在於列表中。")
                    else:
                        print("項目名稱不能為空。")
            elif choice == len(categories):
                break
            else:
                print("無效選擇。")
        except ValueError:
            print("輸入無效，請輸入數字。")


def build_prompt():
    """引導使用者建立完整的提示詞。"""
    print("="*30)
    print("AI 繪圖提示詞建構器 (進階版)")
    print("="*30)
    print("請依序輸入提示詞的各個組成部分。\n")

    # 1. 主要主體 (Subject) - 必填
    subject = get_user_input("1. 主要主體 (例如: a majestic dragon, a futuristic city):")

    # 2. 描述與細節 (Description & Details) - 可選
    description = get_user_input("2. 主體的描述與細節 (例如: with emerald scales, intricate glowing circuits):", allow_empty=True)

    # 3. 動作/姿勢 (Action/Pose) - 可選
    action = get_user_input("3. 主體的動作或姿勢 (例如: soaring through the clouds, playfully chasing):", allow_empty=True)

    # 4. 場景/背景 (Setting/Background) - 可選
    setting = get_user_input("4. 場景或背景 (例如: in a stormy sky, amidst towering skyscrapers):", allow_empty=True)

    print("\n--- 風格與藝術性 ---")
    # 5. 藝術風格 (Artistic Style) - 可選，可多選
    styles = get_choice_from_list("STYLES", "5. 藝術風格", allow_multiple=True, allow_empty=True)

    # 6. 藝術家參考 (Artist Inspiration) - 可選，可多選
    artists = get_choice_from_list("ARTISTS", "6. 藝術家參考", allow_multiple=True, allow_empty=True)

    # 7. 媒介 (Medium) - 可選，可多選
    mediums = get_choice_from_list("MEDIUMS", "7. 藝術媒介", allow_multiple=True, allow_empty=True)

    print("\n--- 視覺效果 ---")
    # 8. 色彩 (Colors) - 可選，可多選
    colors = get_choice_from_list("COLORS", "8. 色彩描述", allow_multiple=True, allow_empty=True)

    # 9. 光照 (Lighting) - 可選，可多選
    lighting = get_choice_from_list("LIGHTING", "9. 光照效果", allow_multiple=True, allow_empty=True)

    # 10. 構圖/視角 (Composition/Camera Angle) - 可選，可多選
    composition = get_choice_from_list("COMPOSITION", "10. 構圖或攝影機視角", allow_multiple=True, allow_empty=True)

    # 11. 品質標籤 (Quality Tags) - 可選，可多選
    quality_tags_list = current_elements.get("QUALITY_TAGS", [])
    # 讓品質標籤也能自訂新增，但預設多選且不允許為空（建議總是要有品質標籤）
    selected_quality_tags = get_choice_from_list("QUALITY_TAGS", "11. 品質標籤 (建議選幾個)", allow_multiple=True, allow_empty=False)
    if not selected_quality_tags and quality_tags_list: # 如果用戶跳過了，隨機選一些
         selected_quality_tags = random.sample(quality_tags_list, k=min(3, len(quality_tags_list)))


    # 12. 額外參數/權重 (Optional: Aspect Ratio, Seed, etc.)
    extra_params = get_user_input("12. 額外參數 (例如 --ar 16:9, --seed 12345) (可選):", allow_empty=True)

    # --- 構建提示詞 ---
    prompt_parts = []

    current_subject_phrase = subject
    if description:
        current_subject_phrase += f", {description}"
    if action:
        current_subject_phrase += f", {action}"
    prompt_parts.append(current_subject_phrase)

    if setting:
        prompt_parts.append(f"in {setting}")

    def add_list_items_to_prompt(items_list):
        if items_list:
            if isinstance(items_list, list):
                prompt_parts.extend(item for item in items_list if item) # 過濾空字串
            elif isinstance(items_list, str) and items_list: # 單選情況
                prompt_parts.append(items_list)

    add_list_items_to_prompt(styles)
    add_list_items_to_prompt(artists)
    add_list_items_to_prompt(mediums)
    add_list_items_to_prompt(colors)
    add_list_items_to_prompt(lighting)
    add_list_items_to_prompt(composition)
    add_list_items_to_prompt(selected_quality_tags)

    final_prompt = ", ".join(filter(None, prompt_parts))

    if extra_params:
        final_prompt += f" {extra_params.strip()}"

    print("\n" + "="*30)
    print("🎉 生成的 AI 繪圖提示詞 🎉")
    print("="*30)
    print(final_prompt)

    # --- 負面提示詞 (Negative Prompt) ---
    print("\n--- 負面提示詞 (Negative Prompt) ---")
    print("負面提示詞用於告訴 AI 避免生成哪些內容。")
    use_negative_prompt = input("是否需要建立負面提示詞? (y/n, 預設 n): ").lower() == 'y'
    negative_prompt_parts = []

    if use_negative_prompt:
        print("\n建議的負面提示詞 (可多選，或自訂):")
        selected_negatives = get_choice_from_list(
            "NEGATIVE_PROMPT_SUGGESTIONS", 
            "選擇或自訂負面提示詞", 
            allow_custom=True,  # 允許用戶自訂新的負面提示詞條目
            allow_multiple=True, 
            allow_empty=True
        )
        if isinstance(selected_negatives, list):
            negative_prompt_parts.extend(selected_negatives)
        elif selected_negatives: # 如果是單選且非空
            negative_prompt_parts.append(selected_negatives)


        custom_negative = get_user_input("還需要補充其他不想出現的負面提示詞嗎? (可選，用逗號分隔):", allow_empty=True)
        if custom_negative:
            negative_prompt_parts.extend([c.strip() for c in custom_negative.split(',') if c.strip()])

    if negative_prompt_parts:
        final_negative_prompt = ", ".join(filter(None, list(set(negative_prompt_parts))))
        print("\n--- 生成的負面提示詞 ---")
        print(final_negative_prompt)
        print("\n(某些 AI 工具可能使用 --no 參數，例如: --no text, signature)")
    else:
        print("未生成負面提示詞。")

    print("\n" + "="*30)
    print("提示詞已生成完畢！")
    print("="*30)

def main_menu():
    """主選單，允許用戶選擇建立提示或管理元素。"""
    load_elements() # 啟動時載入元素
    while True:
        print("\n--- 主選單 ---")
        print("1. 建立新的 AI 繪圖提示詞")
        print("2. 管理自訂元素 (風格、藝術家等)")
        print("3. 離開")
        choice = input("請選擇操作: ").strip()

        if choice == '1':
            build_prompt()
        elif choice == '2':
            manage_elements()
        elif choice == '3':
            print("感謝使用，再見！")
            break
        else:
            print("無效選擇，請重新輸入。")

if __name__ == "__main__":
    main_menu()