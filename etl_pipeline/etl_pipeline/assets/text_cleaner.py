# etl_pipeline/etl_pipeline/assets/text_cleaner.py
import re
import unicodedata

VIETNAMESE_CHARS = (
    "a-zA-ZĐđĂăÂâÊêÔôƠơƯư"
    "ÁáÀàẢảÃãẠạẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặ"
    "ÉéÈèẺẻẼẽẸẹẾếỀềỂểỄễỆệ"
    "ÍíÌìỈỉĨĩỊị"
    "ÓóÒòỎỏÕõỌọỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợ"
    "ÚúÙùỦủŨũỤụỨứỪừỬửỮữỰự"
    "ÝýỲỳỶỷỸỹỴỵ"
)

# Common replacements mapping (typo patterns -> standard words)
COMMON_REPLACEMENTS = {
    # Scan artifacts and spacing
    r"Đ -ông": "Đông",
    r"d -ông": "đông",
    r"đ -ông": "đông",
    r"ỉ^ây": "Đây",
    r"l<à": "là",
    r"\\ à": " và ",
    r"\\’à": " và ",
    r"\\‘à": " và ",
    r"\\’i": "vi",
    r"\\’": "'",
    r"v\.\.\.": "v.v.",
    r"v\.\\\.": "v.v.",
    r"v\.\\\.\.": "v.v.",
    
    # Specific book typos from user
    r"gla đình": "gia đình",
    r"thặy thuốc": "thầy thuốc",
    r"BÁCHkHOA": "BÁCH KHOA",
    r"BÁCH kHOA": "BÁCH KHOA",
    r"VIXAMIN": "VITAMIN",
    r"V I X A M I N": "VITAMIN",
    r"vitẨ^min": "vitamin",
    r"vit Ẩ^m in": "vitamin",
    r"axit floic": "axit folic",
    r"floic": "folic",
    r"thòi kì": "thời kì",
    r"tiếu tiện": "tiểu tiện",
    r"cấu tích": "Cẩu tích",
    r"Câu tích": "Cẩu tích",
    r"yêu = cột sô\);y": "yêu = cột sống",
    r"yêu = cột sô\)": "yêu = cột sống",
    r"bât túc": "bất túc",
    r"dới hạ": "đới hạ",
    r"axit panothenic": "axit pantothenic",
    r"thừimin": "thiamin",
    r"calabasimin": "cobalamin",
    r"lOOg": "100g",
    r"l\.OOOnil": "1.000ml",
    r"l\.500 ml": "1.500 ml",
    r"phcần": "phần",
    r"trcắng": "trắng",
    r"tv giài": "tỳ giải",
    r"tỳ giài": "tỳ giải",
    r"ngâni": "ngâm",
    r"uô\"ng": "uống",
    r"uông": "uống",
    r"utmg": "uống",
    r"đê": "để",
    r"I ỉoặc": "Hoặc",
    r"ỉ ỉoặc": "Hoặc",
    r"tài thiệu": "tài liệu",
    r"c\^liuơng\. 1": "Chương 1",
    r"c\^liuơng": "Chương",
    r"Bdcíi": "Bách",
    r"ịíioa": "khoa",
    r"ịỊioa": "khoa",
    r"retinoiy": "retinol",
    r"yô/ mỏi": "yếu mỏi",
    r"Vitam ỉn": "Vitamin",
    r"Vitamỉn": "Vitamin",
    r"VITmVllN": "VITAMIN",
    r"vitomìn": "vitamin",
    r"nưốc": "nước",
    r"nưóc": "nước",
    r"sông": "sống",
    r"mõ": "mỡ",
    r"đốỉ": "đối",
    r"vối": "với",
    r"thốhg": "thống",
    r"Viatmin": "Vitamin",
    
    # Common words spelling
    r"Ccác": "Các",
    r"ccác": "các",
    r"Nhcà": "Nhà",
    r"XLiât": "Xuất",
    r"dông": "đông",
    r"khcào": "khảo",
    r"thê": "thể",
    r"sử dung": "sử dụng",
    r"quã": "quả",
    r"biỊn": "bạn",
    r"cuôn": "cuốn",
    r"dâv": "đây",
    r"câm": "cẩm",
    r"cúa": "của",
    r"mồi": "mỗi",
    r"vứi": "với",
    r"Tcâv": "Tây",
    r"tcâv": "tây",
    r"Tâv": "Tây",
    r"tâv": "tây",
    r"Dông": "Đông",
    r"Ngủ": "Ngũ",
    r"thuvêt": "thuyết",
    r"thuyêt": "thuyết",
    r"nhât": "nhất",
    r"Mặc dcầu": "Mặc dầu",
    r"hang tượng": "tạng tượng",
    r"hạng tượng": "tạng tượng",
    r"diêm": "điểm",
    r"phôi": "phổi",
    r"mcật": "mật",
    r"khiên cưởng": "khiên cưỡng",
    r"s\.ách": "sách",
    r"ởViệt": "ở Việt",
    r"Dông v": "Đông y",
    
    # TCM terms typos
    r"Tcâm": "Tâm",
    r"Thtận": "Thận",
    r"thtận": "thận",
    r"VỊ": "Vị",
    r"Dảm": "Đởm",
    r"Dại": "Đại",
}

def is_vietnamese_letter(char: str) -> bool:
    """Check if a character is a single Vietnamese alphabetical letter."""
    return len(char) == 1 and bool(re.match(rf'^[{VIETNAMESE_CHARS}]$', char))

def merge_spaced_letters(text: str) -> str:
    """
    Merge sequences of single letters separated by a single space (e.g. "Đ Ầ U" -> "ĐẦU")
    without merging separate words (like "Đông y" or "y học").
    """
    lines = []
    for line in text.splitlines():
        # Split by multiple spaces or tabs first to preserve word boundaries
        parts = re.split(r'([ \t]{2,})', line)
        new_parts = []
        for part in parts:
            if not part.strip():
                new_parts.append(part)
                continue
            
            words = part.split(' ')
            merged_words = []
            temp_word = []
            for w in words:
                if is_vietnamese_letter(w):
                    temp_word.append(w)
                else:
                    if temp_word:
                        merged_words.append(''.join(temp_word))
                        temp_word = []
                    if w:
                        merged_words.append(w)
            if temp_word:
                merged_words.append(''.join(temp_word))
            new_parts.append(' '.join(merged_words))
        lines.append(''.join(new_parts))
    return '\n'.join(lines)

def clean_ocr_text(text: str) -> str:
    if not text:
        return ""
    
    # 1. Normalize Unicode to NFC form
    text = unicodedata.normalize("NFC", text)
    
    # 2. Merge spaced-out words (e.g. "Đ Ầ U" -> "ĐẦU")
    text = merge_spaced_letters(text)
    
    # 3. Apply common replacements
    for pattern, replacement in COMMON_REPLACEMENTS.items():
        text = re.sub(pattern, replacement, text)
        
    # 4. Standard word-boundary corrections (d vs đ, etc.)
    text = re.sub(r'\bdể\b', 'để', text)
    text = re.sub(r'\bdược\b', 'được', text)
    text = re.sub(r'\bdưọc\b', 'được', text)
    text = re.sub(r'\bdưóc\b', 'được', text)
    text = re.sub(r'\bdực\b', 'được', text)
    text = re.sub(r'\bdọc\b', 'đọc', text)
    text = re.sub(r'\bdiều trị\b', 'điều trị', text)
    text = re.sub(r'\bdó\b', 'đó', text)
    text = re.sub(r'\bdất\b', 'đất', text)
    
    # Clean up double/multiple spaces
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()
