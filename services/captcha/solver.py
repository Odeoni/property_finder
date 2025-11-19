"""
Reusable CAPTCHA solving service using CapSolver API
Can be imported and used across multiple scripts in the project
"""

import requests
import time
import re
from typing import Optional, Tuple


class CaptchaSolver:
    """
    Handles CAPTCHA detection and solving using CapSolver API
    Supports: ReCaptcha v2, ReCaptcha v3, hCaptcha
    """
    
    def __init__(self, api_key: str):
        """
        Initialize solver with CapSolver API key
        
        Args:
            api_key: Your CapSolver API key
        """
        self.api_key = api_key
        self.base_url = "https://api.capsolver.com"
    
    def detect_captcha_type(self, page_content: str, page_locator=None) -> Tuple[Optional[str], Optional[str]]:
        """
        Detect which type of CAPTCHA is present on the page
        
        Args:
            page_content: HTML content of the page (string)
            page_locator: Optional Playwright page object for element checking
            
        Returns:
            Tuple of (captcha_type, site_key) or (None, None) if no CAPTCHA found
            
        Example:
            captcha_type, site_key = solver.detect_captcha_type(page.content(), page)
        """
        # ReCaptcha v2 detection
        if page_locator and hasattr(page_locator, 'locator'):
            try:
                if page_locator.locator('.g-recaptcha').count() > 0:
                    site_key = page_locator.locator('.g-recaptcha').get_attribute('data-sitekey')
                    return "ReCaptchaV2TaskProxyLess", site_key
            except:
                pass
        
        # Check HTML content for ReCaptcha v2
        recaptcha_v2_match = re.search(r'class=["\']g-recaptcha["\'][^>]*data-sitekey=["\']([^"\']+)["\']', page_content)
        if recaptcha_v2_match:
            return "ReCaptchaV2TaskProxyLess", recaptcha_v2_match.group(1)
        
        # ReCaptcha v3 detection
        if 'grecaptcha.execute' in page_content:
            match = re.search(r'grecaptcha\.execute\(["\']([^"\']+)["\']', page_content)
            if match:
                return "ReCaptchaV3TaskProxyLess", match.group(1)
        
        # hCaptcha detection
        if page_locator and hasattr(page_locator, 'locator'):
            try:
                if page_locator.locator('.h-captcha').count() > 0:
                    site_key = page_locator.locator('.h-captcha').get_attribute('data-sitekey')
                    return "HCaptchaTaskProxyLess", site_key
            except:
                pass
        
        # Check HTML content for hCaptcha
        hcaptcha_match = re.search(r'class=["\']h-captcha["\'][^>]*data-sitekey=["\']([^"\']+)["\']', page_content)
        if hcaptcha_match:
            return "HCaptchaTaskProxyLess", hcaptcha_match.group(1)
        
        return None, None
    
    def solve(self, captcha_type: str, site_key: str, page_url: str, timeout: int = 180) -> Optional[str]:
        """
        Solve CAPTCHA using CapSolver API
        
        Args:
            captcha_type: Type of CAPTCHA (e.g., "ReCaptchaV2TaskProxyLess")
            site_key: The site key extracted from the page
            page_url: URL of the page with the CAPTCHA
            timeout: Maximum time to wait for solution (seconds)
            
        Returns:
            CAPTCHA solution token (string) or None if failed
            
        Example:
            token = solver.solve("ReCaptchaV2TaskProxyLess", site_key, "https://example.com")
        """
        # Create task
        create_payload = {
            "clientKey": self.api_key,
            "task": {
                "type": captcha_type,
                "websiteURL": page_url,
                "websiteKey": site_key
            }
        }
        
        headers = {"Content-Type": "application/json"}
        
        try:
            response = requests.post(
                f"{self.base_url}/createTask",
                json=create_payload,
                headers=headers
            )
            result = response.json()
            
            if result.get("errorId") != 0:
                error_msg = result.get('errorDescription', 'Unknown error')
                print(f"❌ CapSolver error: {error_msg}")
                return None
            
            task_id = result["taskId"]
            print(f"✓ CAPTCHA task created: {task_id}")
            
            # Poll for result
            get_payload = {"clientKey": self.api_key, "taskId": task_id}
            
            max_attempts = timeout // 3
            for attempt in range(max_attempts):
                time.sleep(3)
                
                result = requests.post(
                    f"{self.base_url}/getTaskResult",
                    json=get_payload,
                    headers=headers
                )
                data = result.json()
                
                if data.get("status") == "ready":
                    token = data["solution"]["gRecaptchaResponse"]
                    print(f"✅ CAPTCHA solved!")
                    return token
                
                if attempt % 10 == 0:
                    print(f"⏳ Still solving... ({attempt * 3}s elapsed)")
            
            print(f"⚠️ CAPTCHA solving timeout after {timeout}s")
            return None
            
        except Exception as e:
            print(f"❌ CAPTCHA solving error: {str(e)}")
            return None
    
    def inject_token(self, page_executor, token: str, captcha_type: str = "recaptcha"):
        """
        Inject solved CAPTCHA token into page
        
        Args:
            page_executor: Playwright page.evaluate function or similar
            token: The solved CAPTCHA token
            captcha_type: Type of CAPTCHA ("recaptcha" or "hcaptcha")
            
        Example (Playwright):
            solver.inject_token(page.evaluate, token)
            
        Example (Selenium):
            solver.inject_token(driver.execute_script, token)
        """
        if captcha_type.lower() == "recaptcha":
            page_executor(f'''() => {{
                const textarea = document.getElementById('g-recaptcha-response');
                if (textarea) {{
                    textarea.innerHTML = "{token}";
                    textarea.value = "{token}";
                }}
            }}''')
        elif captcha_type.lower() == "hcaptcha":
            page_executor(f'''() => {{
                const textarea = document.querySelector('[name="h-captcha-response"]');
                if (textarea) {{
                    textarea.innerHTML = "{token}";
                    textarea.value = "{token}";
                }}
            }}''')
    
    def solve_and_inject(self, page, page_url: str, timeout: int = 180) -> bool:
        """
        All-in-one: Detect, solve, and inject CAPTCHA
        
        Args:
            page: Playwright page object
            page_url: URL of the page with CAPTCHA
            timeout: Maximum time to wait for solution
            
        Returns:
            True if successful, False otherwise
            
        Example:
            if solver.solve_and_inject(page, page.url):
                # CAPTCHA solved, continue...
        """
        print("🔍 Detecting CAPTCHA...")
        
        page_content = page.content()
        captcha_type, site_key = self.detect_captcha_type(page_content, page)
        
        if not captcha_type:
            print("ℹ️ No CAPTCHA detected")
            return True
        
        print(f"🤖 Found {captcha_type}")
        print(f"🔑 Site key: {site_key}")
        
        token = self.solve(captcha_type, site_key, page_url, timeout)
        
        if not token:
            return False
        
        print("💉 Injecting token...")
        self.inject_token(page.evaluate, token)
        
        return True


# Convenience function for quick usage
def solve_captcha_quick(api_key: str, page, page_url: str) -> bool:
    """
    Quick one-liner to solve CAPTCHA
    
    Example:
        from services.captcha.solver import solve_captcha_quick
        solve_captcha_quick(CAPSOLVER_API_KEY, page, page.url)
    """
    solver = CaptchaSolver(api_key)
    return solver.solve_and_inject(page, page_url)