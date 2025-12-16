class Solution:

    def get_matrix_size(self, board):
        num_rows, num_cols = len(board), len(board[0])
        return num_rows, num_cols

    def find_letter(self, board, letter):
        letter_map = list()
        num_rows, num_cols = self.get_matrix_size(board)
        for row in range(num_rows):
            for col in range(num_cols):
                if board[row][col] == letter:
                    letter_map.append((row, col))
        return letter_map
    
    def find_next_letter(self, board, letter, x, y):
        available_moves = [(0, 1), (0, -1), (1, 0), (-1, 0)]
        return

    def find_word(self, board, word):
        letter_map = self.find_letter(board, word[0])
        for x, y in letter_map:
            for letter in word:
                l_x, l_y = self.find_next_letter(board, letter, x, y)

        return

    def find_words(self, board, words):
        for word in words:
            found = self.find_word(board, word)
        return
    


board = [["o","a","a","n"],["e","t","a","e"],["i","h","k","r"],["i","f","l","v"]]
words = ["oath","pea","eat","rain"]
Solution().find_words(board, words)